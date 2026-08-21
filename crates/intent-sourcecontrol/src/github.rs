//! `GitHubSourceControl` — the v1 [`SourceControl`] impl backed by `octocrab`
//! (§7.3, §7.5).
//!
//! REST calls go through octocrab's generic `get`/`post`/`patch`/`put` helpers
//! (returning raw JSON we map ourselves, mirroring the TS ground truth which
//! parsed raw GitHub payloads), and review-thread resolution uses the GraphQL
//! client. octocrab unwraps the GraphQL envelope itself (`data` on success,
//! `errors` mapped onto [`Error::Api`]), so [`graphql_data`] only guards
//! against a `null` payload.

use std::fmt::Write as _;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::error::{Error, Result};
use crate::model::*;
use crate::SourceControl;

/// Bounded TCP connect wait for every octocrab request. Without these
/// timeouts a connection that goes dark makes an in-flight request pend
/// forever, which wedged the serialized PR-monitor sweep — `lastPolledAt`
/// froze for *all* monitors and no wakes were delivered
/// (intent-hq/monorepo#1988).
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Bounded per-read/-write socket wait for every octocrab request; same
/// rationale as [`CONNECT_TIMEOUT`] (intent-hq/monorepo#1988).
pub(crate) const READ_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// GitHub implementation of [`SourceControl`].
pub struct GitHubSourceControl {
    client: octocrab::Octocrab,
}

impl GitHubSourceControl {
    /// Build a client from a personal token, optionally targeting a GitHub
    /// Enterprise instance via `api_base_url` (`octocrab` `.base_uri(...)`).
    ///
    /// # Errors
    ///
    /// Returns an error if the octocrab client cannot be built (e.g. an invalid `api_base_url`).
    pub fn new(token: &str, api_base_url: Option<&str>) -> Result<Self> {
        let mut builder = octocrab::Octocrab::builder()
            .personal_token(token.to_string())
            .set_connect_timeout(Some(CONNECT_TIMEOUT))
            .set_read_timeout(Some(READ_WRITE_TIMEOUT))
            .set_write_timeout(Some(READ_WRITE_TIMEOUT));
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

/// Hard safety cap for exhaustive (non-cursored) REST reads: at most 10 pages
/// of `per_page=100` (1000 items), bounding rate-limit cost if a PR ever
/// accumulates a pathological number of reviews/check runs.
const REST_EXHAUSTIVE_MAX_PAGES: u64 = 10;

/// Whether an exhaustive REST read should fetch the next page: the current
/// page filled the per-page window (same count heuristic as
/// [`rest_next_cursor`]) and the [`REST_EXHAUSTIVE_MAX_PAGES`] cap has not
/// been reached.
fn rest_fetch_next_page(page: u64, fetched: usize, per_page: u64) -> bool {
    page < REST_EXHAUSTIVE_MAX_PAGES && per_page > 0 && fetched as u64 == per_page
}

/// Client-side pagination for endpoints that return the entire result set in
/// one response (GitHub ignores `per_page`/`page` on `matching-refs`,
/// github/docs#3863): slice the 1-based `(page, per_page)` window out of
/// `all` and emit a next cursor only when items remain beyond it — an
/// exactly-full final window therefore ends the listing without the extra
/// empty fetch [`rest_next_cursor`] tolerates.
fn page_full_set<T>(all: Vec<T>, page: u64, per_page: u64) -> Page<T> {
    let offset = page.saturating_sub(1).saturating_mul(per_page);
    let has_more = (all.len() as u64) > page.saturating_mul(per_page);
    let items = all
        .into_iter()
        .skip(offset as usize)
        .take(per_page as usize)
        .collect();
    Page {
        items,
        next_cursor: has_more.then(|| (page + 1).to_string()),
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

/// Map a `GET /git/matching-refs/heads/{prefix}` entry onto a [`Branch`]:
/// `refs/heads/<name>` → `<name>` plus the ref object's SHA. Non-branch refs
/// map to `None` (defensive; the route already scopes to `heads/`). The refs
/// API carries no protection flag, so `protected` defaults to `false`.
pub(crate) fn map_matching_ref(value: Value) -> Result<Option<Branch>> {
    let r: dto::MatchingRef = serde_json::from_value(value)?;
    let full = r.r#ref.unwrap_or_default();
    let Some(name) = full.strip_prefix("refs/heads/") else {
        return Ok(None);
    };
    Ok(Some(Branch {
        name: name.to_string(),
        commit_sha: r.object.and_then(|o| o.sha),
        protected: false,
    }))
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

/// Blank-stripped free-text search term: `None` when absent or whitespace-only
/// so a blank `search` never changes the listing route.
fn search_term(search: Option<&str>) -> Option<&str> {
    search.map(str::trim).filter(|t| !t.is_empty())
}

/// Neutralize search-syntax escapes in free text so it cannot widen the
/// `repo:{o}/{r}` scope the builders prefix: embedded quotes are stripped and
/// any token carrying qualifier (`:`) or boolean-operator (`OR`/`AND`/`NOT`)
/// semantics is double-quoted, which GitHub search treats as a literal term.
fn sanitize_search_text(text: &str) -> String {
    text.split_whitespace()
        .map(|token| {
            let token = token.replace('"', "");
            if token.contains(':') || matches!(token.as_str(), "OR" | "AND" | "NOT") {
                format!("\"{token}\"")
            } else {
                token
            }
        })
        .filter(|t| !t.is_empty() && t != "\"\"")
        .collect::<Vec<_>>()
        .join(" ")
}

/// Build the `GET /search/issues` query for a PR search: `is:pr repo:{o}/{r}
/// is:{state}` plus the optional `@me` involvement clause (parity with the FE
/// `searchGitHubPullRequests`) and the optional free-text term.
pub(crate) fn build_pr_search_query(
    repo: &RepoRef,
    state: &str,
    involvement: Option<PrInvolvement>,
    search: Option<&str>,
) -> String {
    let mut q = format!("is:pr repo:{}/{} is:{state}", repo.owner, repo.name);
    if let Some(involvement) = involvement {
        let involve = match involvement {
            PrInvolvement::Created => "author:@me",
            PrInvolvement::Assigned => "assignee:@me",
            PrInvolvement::ReviewRequested => "review-requested:@me",
            PrInvolvement::Involves => "involves:@me",
        };
        q.push(' ');
        q.push_str(involve);
    }
    if let Some(text) = search_term(search) {
        let text = sanitize_search_text(text);
        if !text.is_empty() {
            q.push(' ');
            q.push_str(&text);
        }
    }
    q
}

/// Build the `GET /search/issues` query for an issue search: `is:issue
/// repo:{o}/{r}` plus a `state:` clause (`all` adds none), a `label:` clause
/// per comma-separated label (parity with the `/issues` listing `labels`
/// param), and the free-text term.
pub(crate) fn build_issue_search_query(
    repo: &RepoRef,
    state: &str,
    labels: Option<&str>,
    search: &str,
) -> String {
    let mut q = format!("is:issue repo:{}/{}", repo.owner, repo.name);
    if matches!(state, "open" | "closed") {
        let _ = write!(q, " state:{state}");
    }
    for label in labels
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|l| !l.is_empty())
    {
        let _ = write!(q, " label:\"{}\"", label.replace('"', ""));
    }
    let text = sanitize_search_text(search);
    if !text.is_empty() {
        q.push(' ');
        q.push_str(&text);
    }
    q
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

/// Decode a contents-API **file** payload (`GET /repos/{o}/{r}/contents/{path}`)
/// into its UTF-8 text. GitHub returns `{ "type": "file", "encoding": "base64",
/// "content": "<base64 wrapped at 60 cols>" }`; the wrap whitespace is stripped
/// before decoding. Directory (array) payloads and non-base64 encodings are
/// decode errors — the caller asked for one file's text.
fn decode_contents_file(v: &Value) -> Result<String> {
    use base64::Engine as _;
    if v.is_array() {
        return Err(Error::Decode(
            "contents path is a directory, not a file".to_string(),
        ));
    }
    let content = v
        .get("content")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Decode("contents payload missing string `content`".to_string()))?;
    let encoding = v
        .get("encoding")
        .and_then(Value::as_str)
        .unwrap_or("base64");
    if encoding != "base64" {
        return Err(Error::Decode(format!(
            "unsupported contents encoding {encoding:?}"
        )));
    }
    let compact: String = content.chars().filter(|c| !c.is_whitespace()).collect();
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(compact)
        .map_err(|e| Error::Decode(format!("invalid base64 in contents payload: {e}")))?;
    String::from_utf8(bytes)
        .map_err(|e| Error::Decode(format!("contents payload is not UTF-8: {e}")))
}

/// Percent-encode a repo-relative path for use in a REST route, keeping `/`
/// as the segment separator. Everything outside RFC 3986 unreserved
/// (`A-Z a-z 0-9 - . _ ~`) is `%XX`-encoded byte-wise so paths with spaces,
/// `#`, `?`, `%`, etc. cannot break or redirect the route.
fn encode_path_segments(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for byte in path.bytes() {
        match byte {
            b'/' | b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char);
            }
            _ => {
                let _ = write!(out, "%{byte:02X}");
            }
        }
    }
    out
}

/// Validate the `data` payload of a GraphQL response.
///
/// `octocrab::Octocrab::graphql` already unwraps the envelope — it returns the
/// `data` object on success and turns an `errors` array into
/// `octocrab::Error::Graphql` (mapped onto [`Error::Api`]) — so the only
/// remaining failure mode is a `null` payload with no accompanying error.
fn graphql_data(data: Value) -> Result<Value> {
    if data.is_null() {
        return Err(Error::Api("graphql response returned no data".to_string()));
    }
    Ok(data)
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
    pub(super) struct RefObject {
        pub sha: Option<String>,
    }

    /// A `GET /git/matching-refs/{ref}` entry: the fully-qualified ref name
    /// plus the object it points at.
    #[derive(Deserialize)]
    pub(super) struct MatchingRef {
        #[serde(rename = "ref")]
        pub r#ref: Option<String>,
        pub object: Option<RefObject>,
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

const REVIEW_DECISION_QUERY: &str = r"
query GetReviewDecision($owner: String!, $repo: String!, $prNumber: Int!) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $prNumber) {
      reviewDecision
    }
  }
}
";

/// Map the GraphQL `reviewDecision` payload to [`ReviewDecision`]. GitHub
/// reports `null` when the base branch has no review requirement; that (or
/// any unrecognized value) maps to `None`.
fn parse_review_decision(data: &Value) -> Option<ReviewDecision> {
    match data
        .pointer("/repository/pullRequest/reviewDecision")
        .and_then(Value::as_str)
    {
        Some("APPROVED") => Some(ReviewDecision::Approved),
        Some("CHANGES_REQUESTED") => Some(ReviewDecision::ChangesRequested),
        Some("REVIEW_REQUIRED") => Some(ReviewDecision::ReviewRequired),
        _ => None,
    }
}

/// Known ceiling: `contexts(first: 100)` is a single unpaginated page, so a
/// PR whose rollup exceeds 100 contexts (very large CI matrices) silently
/// truncates — checks beyond the page are invisible to the requirements
/// probe, and a monitor diffing two truncated pages can report phantom
/// "check removed" lines for whatever fell off. Paginating `contexts` is the
/// complete fix if that ceiling is ever hit in practice.
const MERGE_REQUIREMENTS_QUERY: &str = r"
query GetMergeRequirements($owner: String!, $repo: String!, $prNumber: Int!) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $prNumber) {
      mergeStateStatus
      reviewDecision
      baseRefName
      commits(last: 1) {
        nodes {
          commit {
            statusCheckRollup {
              contexts(first: 100) {
                nodes {
                  __typename
                  ... on CheckRun {
                    name
                    status
                    conclusion
                    detailsUrl
                    isRequired(pullRequestNumber: $prNumber)
                  }
                  ... on StatusContext {
                    context
                    state
                    targetUrl
                    isRequired(pullRequestNumber: $prNumber)
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
";

/// The GraphQL pointer to the PR's status-check rollup contexts (the last
/// commit on the PR is its head).
const ROLLUP_CONTEXTS_POINTER: &str =
    "/repository/pullRequest/commits/nodes/0/commit/statusCheckRollup/contexts/nodes";

/// Map a legacy commit-status `state` (`StatusState`) onto [`CheckState`].
/// `EXPECTED`/`PENDING` are still running; `ERROR`/`FAILURE` fail.
fn derive_status_context_state(state: &str) -> CheckState {
    match state {
        "SUCCESS" => CheckState::Success,
        "EXPECTED" | "PENDING" => CheckState::Pending,
        _ => CheckState::Failure,
    }
}

/// Map one `statusCheckRollup.contexts` node onto a [`RollupCheck`]. GraphQL
/// `CheckRun` carries `status`/`conclusion` in `SCREAMING_SNAKE_CASE` (unlike
/// the lowercase REST payload [`derive_check_state`] expects), so the values
/// are lowercased before mapping; `StatusContext` uses the legacy `state`.
fn map_rollup_context(value: &Value) -> Option<RollupCheck> {
    let is_required = value
        .get("isRequired")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let url = |key: &str| value.get(key).and_then(Value::as_str).map(String::from);
    match value.get("__typename").and_then(Value::as_str) {
        Some("StatusContext") => Some(RollupCheck {
            name: value
                .get("context")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            state: derive_status_context_state(
                value
                    .get("state")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            ),
            is_required,
            url: url("targetUrl"),
        }),
        Some("CheckRun") => {
            let status = value
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_ascii_lowercase();
            let conclusion = value
                .get("conclusion")
                .and_then(Value::as_str)
                .map(str::to_ascii_lowercase);
            Some(RollupCheck {
                name: value
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                state: derive_check_state(&status, conclusion.as_deref()),
                is_required,
                url: url("detailsUrl"),
            })
        }
        _ => None,
    }
}

/// Map the `GET /repos/{o}/{r}/rules/branches/{branch}` payload (a flat array
/// of the rules that apply to the branch) onto the merge-relevant subset.
/// Unknown rule types are ignored; the strictest value wins when several
/// rulesets impose the same rule.
fn map_branch_rules(value: &Value) -> BranchRules {
    let mut rules = BranchRules::default();
    let Some(items) = value.as_array() else {
        return rules;
    };
    for item in items {
        let params = item.get("parameters");
        match item.get("type").and_then(Value::as_str) {
            Some("pull_request") => {
                if let Some(count) = params
                    .and_then(|p| p.get("required_approving_review_count"))
                    .and_then(Value::as_u64)
                {
                    let count = count as u32;
                    rules.required_approving_review_count = Some(
                        rules
                            .required_approving_review_count
                            .map_or(count, |prev| prev.max(count)),
                    );
                }
                if let Some(required) = params
                    .and_then(|p| p.get("required_review_thread_resolution"))
                    .and_then(Value::as_bool)
                {
                    rules.required_conversation_resolution =
                        Some(rules.required_conversation_resolution.unwrap_or(false) || required);
                }
            }
            Some("required_status_checks") => {
                let checks = params
                    .and_then(|p| p.get("required_status_checks"))
                    .and_then(Value::as_array)
                    .map(Vec::as_slice)
                    .unwrap_or_default();
                for check in checks {
                    if let Some(context) = check.get("context").and_then(Value::as_str) {
                        if !rules.required_status_checks.iter().any(|c| c == context) {
                            rules.required_status_checks.push(context.to_string());
                        }
                    }
                }
            }
            _ => {}
        }
    }
    rules
}

const REVIEW_THREADS_QUERY: &str = r"
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
";

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
        prefix: Option<&str>,
        page: PageParams,
    ) -> Result<Page<Branch>> {
        let per_page = rest_per_page(page.limit);
        let page_no = rest_page(page.cursor.as_deref());
        let params: Vec<(&str, String)> = vec![
            ("per_page", per_page.to_string()),
            ("page", page_no.to_string()),
        ];
        if let Some(prefix) = prefix.filter(|p| !p.is_empty()) {
            // Server-side prefix search via the git refs API
            // (`GET /git/matching-refs/heads/{prefix}`). GitHub ignores
            // `per_page`/`page` on this endpoint and returns the entire
            // match set (github/docs#3863), so the `(page, per_page)`
            // window is applied client-side over the deterministic
            // (ref-ordered) full set after the defensive non-`refs/heads/`
            // filter.
            let route = format!(
                "/repos/{owner}/{name}/git/matching-refs/heads/{}",
                encode_path_segments(prefix)
            );
            let v: Value = self.client.get(&route, None::<&()>).await?;
            let items: Vec<Value> = serde_json::from_value(v)?;
            let branches: Vec<Branch> = items
                .into_iter()
                .map(map_matching_ref)
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect();
            return Ok(page_full_set(branches, page_no, per_page));
        }
        let route = format!("/repos/{owner}/{name}/branches");
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

    async fn get_file_content(
        &self,
        repo: &RepoRef,
        path: &str,
        git_ref: Option<&str>,
    ) -> Result<Option<String>> {
        let route = Self::repo_path(repo, &format!("/contents/{}", encode_path_segments(path)));
        let params: Option<Vec<(&str, String)>> = git_ref.map(|r| vec![("ref", r.to_string())]);
        let v: Value = match self.client.get(&route, params.as_ref()).await {
            Ok(v) => v,
            Err(e) => {
                // 404 (file/repo/ref absent) is the graceful "no such file"
                // outcome, not an error.
                return match Error::from(e) {
                    Error::NotFound(_) => Ok(None),
                    other => Err(other),
                };
            }
        };
        decode_contents_file(&v).map(Some)
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
        let search = search_term(query.search.as_deref());
        if query.involvement.is_some() || search.is_some() {
            // GitHub's `/pulls` listing cannot express assignee/review-requested/
            // involves @me or free text, so route involvement/free-text queries
            // through `/search/issues` (parity with the FE
            // `searchGitHubPullRequests`).
            let state = match query.state {
                Some(PrState::Closed) => "closed",
                Some(PrState::Merged) => "merged",
                _ => "open",
            };
            let q = build_pr_search_query(repo, state, query.involvement, search);
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
        let route = Self::repo_path(repo, &format!("/pulls/{number}/reviews"));
        self.rest_collect_all(&route, |v| v, map_review).await
    }

    async fn review_decision(&self, repo: &RepoRef, number: u64) -> Result<Option<ReviewDecision>> {
        let payload = json!({
            "query": REVIEW_DECISION_QUERY,
            "variables": {
                "owner": repo.owner,
                "repo": repo.name,
                "prNumber": number,
            },
        });
        let resp: Value = self.client.graphql(&payload).await?;
        let data = graphql_data(resp)?;
        Ok(parse_review_decision(&data))
    }

    async fn merge_requirements(
        &self,
        repo: &RepoRef,
        number: u64,
    ) -> Result<MergeRequirementSignals> {
        let payload = json!({
            "query": MERGE_REQUIREMENTS_QUERY,
            "variables": {
                "owner": repo.owner,
                "repo": repo.name,
                "prNumber": number,
            },
        });
        let resp: Value = self.client.graphql(&payload).await?;
        let data = graphql_data(resp)?;
        let pr = data.pointer("/repository/pullRequest");
        let merge_state_status = pr
            .and_then(|p| p.get("mergeStateStatus"))
            .and_then(Value::as_str)
            .map(String::from);
        let rollup = data
            .pointer(ROLLUP_CONTEXTS_POINTER)
            .and_then(Value::as_array);
        let checks = rollup
            .map(|nodes| nodes.iter().filter_map(map_rollup_context).collect())
            .unwrap_or_default();

        // The base branch's rules are a separate REST read whose endpoint may
        // be unreadable (older GHES, a token without the scope); that degrades
        // to `None` instead of failing the probe.
        let branch_rules = match pr
            .and_then(|p| p.get("baseRefName"))
            .and_then(Value::as_str)
            .filter(|b| !b.is_empty())
        {
            Some(base) => {
                let route = Self::repo_path(
                    repo,
                    &format!("/rules/branches/{}", encode_path_segments(base)),
                );
                match self.client.get::<Value, _, ()>(&route, None::<&()>).await {
                    Ok(v) => Some(map_branch_rules(&v)),
                    Err(e) => {
                        tracing::debug!(
                            error = %e,
                            pr_number = number,
                            "merge_requirements: branch rules unreadable, degrading"
                        );
                        None
                    }
                }
            }
            None => None,
        };

        Ok(MergeRequirementSignals {
            merge_state_status,
            review_decision: parse_review_decision(&data),
            checks,
            checks_known: rollup.is_some(),
            branch_rules,
        })
    }

    // Known ceiling: a single `per_page=100` page (newest first), not a full
    // pagination walk. Past 100 conversation comments the count plateaus, so
    // consumers diffing the count for change detection (the PR monitor's
    // `conversationCount`) stop seeing new-comment changes on such PRs.
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
        // The check-runs payload nests the item array under `check_runs`.
        self.rest_collect_all(
            &route,
            |v| {
                v.get("check_runs")
                    .cloned()
                    .unwrap_or_else(|| Value::Array(Vec::new()))
            },
            map_check_run,
        )
        .await
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
        if let Some(search) = search_term(query.search.as_deref()) {
            // The `/issues` listing cannot express free text, so route search
            // queries through `/search/issues` (mirror of the `list_prs`
            // involvement branch).
            let state = query.state.as_deref().unwrap_or("open");
            let q = build_issue_search_query(repo, state, query.labels.as_deref(), search);
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
            // `is:issue` already excludes PRs server-side; the `pull_request`
            // filter stays as defense-in-depth (paging is measured on the raw
            // page, parity with the listing path below).
            let issues = items
                .into_iter()
                .filter(|i| i.get("pull_request").is_none())
                .map(map_issue)
                .collect::<Result<Vec<_>>>()?;
            return Ok(Page {
                items: issues,
                next_cursor: rest_next_cursor(page_no, fetched, per_page),
            });
        }
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
    /// Fetch a REST listing to exhaustion: request `per_page=100` pages from
    /// page 1, extract each page's item array with `extract`, map items with
    /// `map`, and stop on a short page or at the [`REST_EXHAUSTIVE_MAX_PAGES`]
    /// safety cap (see [`rest_fetch_next_page`]).
    async fn rest_collect_all<T>(
        &self,
        route: &str,
        extract: impl Fn(Value) -> Value,
        map: impl Fn(Value) -> Result<T>,
    ) -> Result<Vec<T>> {
        let per_page = REST_MAX_PER_PAGE as u64;
        let mut page = 1u64;
        let mut out = Vec::new();
        loop {
            let params: Vec<(&str, String)> = vec![
                ("per_page", per_page.to_string()),
                ("page", page.to_string()),
            ];
            let v: Value = self.client.get(route, Some(&params)).await?;
            let items: Vec<Value> = serde_json::from_value(extract(v))?;
            let fetched = items.len();
            for item in items {
                out.push(map(item)?);
            }
            if !rest_fetch_next_page(page, fetched, per_page) {
                break;
            }
            page += 1;
        }
        Ok(out)
    }

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
    fn graphql_data_passes_payload_and_rejects_null() {
        // octocrab hands us the already-unwrapped `data` payload.
        let data = graphql_data(json!({ "repository": { "x": 1 } })).unwrap();
        assert_eq!(data, json!({ "repository": { "x": 1 } }));

        let no_data = graphql_data(Value::Null).unwrap_err();
        assert!(matches!(no_data, Error::Api(_)));
    }

    #[test]
    fn parses_review_decision_including_null() {
        let data = |v: Value| json!({ "repository": { "pullRequest": { "reviewDecision": v } } });
        assert_eq!(
            parse_review_decision(&data(json!("APPROVED"))),
            Some(ReviewDecision::Approved)
        );
        assert_eq!(
            parse_review_decision(&data(json!("CHANGES_REQUESTED"))),
            Some(ReviewDecision::ChangesRequested)
        );
        assert_eq!(
            parse_review_decision(&data(json!("REVIEW_REQUIRED"))),
            Some(ReviewDecision::ReviewRequired)
        );
        // Null (unprotected base: no review requirement) and unrecognized
        // values map to None, as does a missing pullRequest node.
        assert_eq!(parse_review_decision(&data(Value::Null)), None);
        assert_eq!(parse_review_decision(&data(json!("SOMETHING_NEW"))), None);
        assert_eq!(
            parse_review_decision(&json!({ "repository": { "pullRequest": null } })),
            None
        );
    }

    #[test]
    fn maps_rollup_contexts_from_both_variants() {
        let check = map_rollup_context(&json!({
            "__typename": "CheckRun",
            "name": "build",
            "status": "COMPLETED",
            "conclusion": "SUCCESS",
            "detailsUrl": "https://ci/run/1",
            "isRequired": true
        }))
        .unwrap();
        assert_eq!(check.name, "build");
        assert_eq!(check.state, CheckState::Success);
        assert!(check.is_required);
        assert_eq!(check.url.as_deref(), Some("https://ci/run/1"));

        // An in-flight check-run is pending regardless of conclusion.
        let pending = map_rollup_context(&json!({
            "__typename": "CheckRun",
            "name": "e2e",
            "status": "IN_PROGRESS",
            "conclusion": null,
            "isRequired": false
        }))
        .unwrap();
        assert_eq!(pending.state, CheckState::Pending);
        assert!(!pending.is_required);

        let status = map_rollup_context(&json!({
            "__typename": "StatusContext",
            "context": "ci/legacy",
            "state": "FAILURE",
            "targetUrl": "https://ci/legacy",
            "isRequired": true
        }))
        .unwrap();
        assert_eq!(status.name, "ci/legacy");
        assert_eq!(status.state, CheckState::Failure);
        assert!(status.is_required);

        // Unknown union members are skipped rather than mis-mapped.
        assert!(map_rollup_context(&json!({ "__typename": "Something" })).is_none());
    }

    #[test]
    fn maps_branch_rules_subset_and_ignores_unknown_types() {
        let rules = map_branch_rules(&json!([
            { "type": "deletion" },
            {
                "type": "pull_request",
                "parameters": {
                    "required_approving_review_count": 1,
                    "required_review_thread_resolution": true
                }
            },
            {
                "type": "pull_request",
                "parameters": { "required_approving_review_count": 2 }
            },
            {
                "type": "required_status_checks",
                "parameters": {
                    "required_status_checks": [
                        { "context": "build" },
                        { "context": "test" },
                        { "context": "build" }
                    ]
                }
            }
        ]));
        // The strictest approval count across rulesets wins; contexts dedupe.
        assert_eq!(rules.required_approving_review_count, Some(2));
        assert_eq!(rules.required_conversation_resolution, Some(true));
        assert_eq!(
            rules.required_status_checks,
            vec!["build".to_string(), "test".to_string()]
        );

        // An empty ruleset array reports no rules (not an error).
        assert_eq!(map_branch_rules(&json!([])), BranchRules::default());
        assert_eq!(map_branch_rules(&json!({})), BranchRules::default());
    }

    #[test]
    fn merge_requirement_signals_serialize_camel_case() {
        let signals = MergeRequirementSignals {
            merge_state_status: Some("BLOCKED".into()),
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
        };
        let wire = serde_json::to_value(&signals).unwrap();
        assert_eq!(wire["mergeStateStatus"], "BLOCKED");
        assert_eq!(wire["reviewDecision"], "review_required");
        assert_eq!(wire["checksKnown"], true);
        assert_eq!(wire["checks"][0]["isRequired"], true);
        assert_eq!(
            wire["branchRules"]["requiredApprovingReviewCount"],
            json!(1)
        );
        assert_eq!(
            wire["branchRules"]["requiredConversationResolution"],
            json!(true)
        );
        assert_eq!(
            wire["branchRules"]["requiredStatusChecks"],
            json!(["build"])
        );
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
    fn maps_matching_ref_fixture() {
        // `refs/heads/<name>` → branch name (slashes in the name preserved),
        // SHA from the ref object, and no protection flag on the refs API.
        let b = map_matching_ref(json!({
            "ref": "refs/heads/feature/login",
            "object": { "sha": "deadbeef", "type": "commit" }
        }))
        .unwrap()
        .expect("branch ref maps");
        assert_eq!(b.name, "feature/login");
        assert_eq!(b.commit_sha.as_deref(), Some("deadbeef"));
        assert!(!b.protected);
    }

    #[test]
    fn matching_ref_skips_non_branch_refs_and_defaults_sha() {
        // Defensive: a non-`refs/heads/` ref maps to `None` instead of a
        // mangled branch name.
        assert!(map_matching_ref(json!({ "ref": "refs/tags/v1.0.0" }))
            .unwrap()
            .is_none());
        let b = map_matching_ref(json!({ "ref": "refs/heads/dev" }))
            .unwrap()
            .expect("branch ref maps");
        assert_eq!(b.name, "dev");
        assert!(b.commit_sha.is_none());
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
    fn client_side_pagination_of_full_match_set() {
        // `matching-refs` returns the whole match set, so the window is cut
        // client-side: page 1 takes the first `per_page`, page 2 the rest.
        let all = || vec![1, 2, 3, 4, 5];
        let p1 = page_full_set(all(), 1, 2);
        assert_eq!(p1.items, vec![1, 2]);
        assert_eq!(p1.next_cursor.as_deref(), Some("2"));
        let p2 = page_full_set(all(), 2, 2);
        assert_eq!(p2.items, vec![3, 4]);
        assert_eq!(p2.next_cursor.as_deref(), Some("3"));
        let p3 = page_full_set(all(), 3, 2);
        assert_eq!(p3.items, vec![5]);
        assert_eq!(p3.next_cursor, None);
        // An exactly-full final window ends the listing with no next cursor
        // (no trailing empty fetch, and no repeat of the same results).
        let exact = page_full_set(vec![1, 2], 1, 2);
        assert_eq!(exact.items, vec![1, 2]);
        assert_eq!(exact.next_cursor, None);
        // A window larger than the set returns everything, once.
        let small = page_full_set(vec![1], 1, 100);
        assert_eq!(small.items, vec![1]);
        assert_eq!(small.next_cursor, None);
        // Pages past the end are empty with no next cursor.
        let past = page_full_set(all(), 4, 2);
        assert!(past.items.is_empty());
        assert_eq!(past.next_cursor, None);
        let empty = page_full_set(Vec::<i32>::new(), 1, 2);
        assert!(empty.items.is_empty());
        assert_eq!(empty.next_cursor, None);
    }

    #[test]
    fn rest_exhaustive_page_loop_termination() {
        // A full page continues to the next one.
        assert!(rest_fetch_next_page(1, 100, 100));
        assert!(rest_fetch_next_page(5, 100, 100));
        // A short or empty page stops the loop.
        assert!(!rest_fetch_next_page(1, 40, 100));
        assert!(!rest_fetch_next_page(1, 0, 100));
        // The safety cap stops the loop even on a full page: the last page
        // fetched is `REST_EXHAUSTIVE_MAX_PAGES`.
        assert!(rest_fetch_next_page(
            REST_EXHAUSTIVE_MAX_PAGES - 1,
            100,
            100
        ));
        assert!(!rest_fetch_next_page(REST_EXHAUSTIVE_MAX_PAGES, 100, 100));
        assert!(!rest_fetch_next_page(
            REST_EXHAUSTIVE_MAX_PAGES + 1,
            100,
            100
        ));
    }

    #[test]
    fn maps_user_identity_and_omits_credentials() {
        let u = map_user_identity(json!({
            "login": "octocat",
            "id": 583_231,
            "name": "The Octocat",
            "avatar_url": "https://avatars.githubusercontent.com/u/583231",
            "html_url": "https://github.com/octocat"
        }))
        .unwrap();
        assert_eq!(u.login, "octocat");
        assert_eq!(u.id, Some(583_231));
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
    fn builds_pr_search_query() {
        let repo = RepoRef::new("o", "r");
        // Involvement-only (the pre-existing branch shape).
        assert_eq!(
            build_pr_search_query(&repo, "open", Some(PrInvolvement::Created), None),
            "is:pr repo:o/r is:open author:@me"
        );
        // Free text combines with involvement and state.
        assert_eq!(
            build_pr_search_query(
                &repo,
                "closed",
                Some(PrInvolvement::ReviewRequested),
                Some("panic on save")
            ),
            "is:pr repo:o/r is:closed review-requested:@me panic on save"
        );
        // Free text without involvement still searches; text is trimmed.
        assert_eq!(
            build_pr_search_query(&repo, "merged", None, Some("  flaky test  ")),
            "is:pr repo:o/r is:merged flaky test"
        );
        // Blank text adds no clause.
        assert_eq!(
            build_pr_search_query(&repo, "open", Some(PrInvolvement::Involves), Some("   ")),
            "is:pr repo:o/r is:open involves:@me"
        );
    }

    #[test]
    fn builds_issue_search_query() {
        let repo = RepoRef::new("o", "r");
        assert_eq!(
            build_issue_search_query(&repo, "open", None, "login bug"),
            "is:issue repo:o/r state:open login bug"
        );
        assert_eq!(
            build_issue_search_query(&repo, "closed", None, "crash"),
            "is:issue repo:o/r state:closed crash"
        );
        // `all` carries no state clause; text is trimmed.
        assert_eq!(
            build_issue_search_query(&repo, "all", None, "  crash  "),
            "is:issue repo:o/r crash"
        );
        // Comma-separated labels become quoted `label:` clauses.
        assert_eq!(
            build_issue_search_query(&repo, "open", Some("bug, needs triage ,"), "crash"),
            "is:issue repo:o/r state:open label:\"bug\" label:\"needs triage\" crash"
        );
    }

    #[test]
    fn search_term_strips_blank_input() {
        assert_eq!(search_term(None), None);
        assert_eq!(search_term(Some("")), None);
        assert_eq!(search_term(Some("   ")), None);
        assert_eq!(search_term(Some("  x  ")), Some("x"));
    }

    #[test]
    fn sanitizes_search_syntax_in_free_text() {
        // Plain text passes through (whitespace collapsed).
        assert_eq!(sanitize_search_text("login  bug"), "login bug");
        // Qualifier-shaped tokens are quoted so they cannot widen the
        // builder-owned `repo:` scope.
        assert_eq!(
            sanitize_search_text("crash repo:other/repo"),
            "crash \"repo:other/repo\""
        );
        // Boolean operators are quoted into literals.
        assert_eq!(sanitize_search_text("a OR b"), "a \"OR\" b");
        assert_eq!(sanitize_search_text("NOT ready"), "\"NOT\" ready");
        // Embedded quotes are stripped before quoting.
        assert_eq!(sanitize_search_text("\"repo:x/y\" fix"), "\"repo:x/y\" fix");
        // The builders keep the scope prefix intact around sanitized text.
        let repo = RepoRef::new("o", "r");
        assert_eq!(
            build_issue_search_query(&repo, "open", None, "x repo:evil/evil"),
            "is:issue repo:o/r state:open x \"repo:evil/evil\""
        );
        assert_eq!(
            build_pr_search_query(&repo, "open", None, Some("a OR org:evil")),
            "is:pr repo:o/r is:open a \"OR\" \"org:evil\""
        );
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

    #[test]
    fn decodes_contents_file_payload() {
        // "hello world\n" base64-encoded, wrapped the way GitHub wraps it.
        let v = json!({
            "type": "file",
            "encoding": "base64",
            "content": "aGVsbG8g\nd29ybGQK\n",
        });
        assert_eq!(decode_contents_file(&v).unwrap(), "hello world\n");
    }

    #[test]
    fn decodes_contents_file_defaults_to_base64_encoding() {
        let v = json!({ "content": "e30=" });
        assert_eq!(decode_contents_file(&v).unwrap(), "{}");
    }

    #[test]
    fn contents_directory_payload_is_decode_error() {
        let v = json!([{ "type": "file", "name": "a.txt" }]);
        let err = decode_contents_file(&v).unwrap_err();
        assert!(matches!(err, Error::Decode(_)), "got {err:?}");
    }

    #[test]
    fn contents_missing_content_is_decode_error() {
        let v = json!({ "type": "file", "encoding": "base64" });
        assert!(matches!(
            decode_contents_file(&v).unwrap_err(),
            Error::Decode(_)
        ));
    }

    #[test]
    fn contents_unsupported_encoding_is_decode_error() {
        let v = json!({ "content": "abc", "encoding": "none" });
        assert!(matches!(
            decode_contents_file(&v).unwrap_err(),
            Error::Decode(_)
        ));
    }

    #[test]
    fn contents_invalid_base64_is_decode_error() {
        let v = json!({ "content": "!!!not-base64!!!", "encoding": "base64" });
        assert!(matches!(
            decode_contents_file(&v).unwrap_err(),
            Error::Decode(_)
        ));
    }

    #[test]
    fn contents_non_utf8_is_decode_error() {
        // 0xFF 0xFE is not valid UTF-8.
        let v = json!({ "content": "//4=", "encoding": "base64" });
        assert!(matches!(
            decode_contents_file(&v).unwrap_err(),
            Error::Decode(_)
        ));
    }

    #[test]
    fn encodes_path_segments_for_contents_route() {
        // Slashes stay as separators; unreserved chars pass through.
        assert_eq!(
            encode_path_segments(".intent/config.json"),
            ".intent/config.json"
        );
        // Spaces, `#`, `?`, `%` cannot break or redirect the route.
        assert_eq!(
            encode_path_segments("dir name/f#1?x=2 50%.json"),
            "dir%20name/f%231%3Fx%3D2%2050%25.json"
        );
    }
}
