//! Native GitLab reads and same-project API creation. No secret-store or CLI access.
//!
//! Read conversion and pagination adapted from Bert Colemont's Apache-2.0
//! contribution in anubissbe/intentd (dcd9150b, 3c9e692c). Credential injection,
//! descriptors, typed availability and creation boundaries are Intent adaptations.
use std::{
    sync::{Arc, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use reqwest::{header::HeaderMap, Method, Url};
use secrecy::{ExposeSecret, SecretString};
use serde_json::{json, Value};

use crate::{
    error::{ProviderFailure, ProviderFailureKind},
    instance::{GitlabDescriptor, GitlabInstance},
    model::{
        AuthStatus, Branch, BranchRules, CheckRun, CheckState, Comment, CommentAnchor, Issue,
        IssueQuery, MergeMethod, MergeOptions, MergeOutcome, MergeRequirementSignals, Mergeability,
        NewPullRequest, Page, PageParams, PrInvolvement, PrObservation, PrPatch, PrQuery, PrState,
        PullRequest, RateLimitStatus, Repo, RepoRef, Review, ReviewComment, ReviewDecision,
        ReviewThread, ReviewThreadComment, ReviewThreadTally, ReviewVerdict, RollupCheck,
        RollupCheckKind, ScCapabilities, UserIdentity,
    },
    model::{
        ProviderAvailability, ReviewAvailability, ReviewBranchIdentity, ReviewDetails,
        ReviewObservation,
    },
    Error, Result, SourceControl,
};

mod http;
use http::{failure, Purpose, RequestScope};
mod provenance;
use provenance::RequestProvenance;
mod creation;
mod observation;
mod search;

const MAX_PAGES: u32 = 100;
const PIPELINE_CHECK: &str = "GitLab pipeline";
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

/// Supplies a fresh credential for each request within the caller's admitted scope.
/// Implementations must recheck connection/account/generation and refresh under the
/// shared lifetime owner. This provider never reads settings, secrets or a CLI.
#[async_trait]
pub trait GitlabRequestCredentials: Send + Sync {
    /// Obtain the current token for this logical instance, or reject a retired scope.
    async fn token_for(&self, instance: &GitlabInstance) -> Result<SecretString>;

    /// Revalidate the actual request before releasing its token. The default
    /// preserves older injected callbacks; qualified adapters override this.
    async fn token_for_request(
        &self,
        instance: &GitlabInstance,
        request: GitlabCredentialRequest<'_>,
    ) -> Result<SecretString> {
        let _ = request;
        self.token_for(instance).await
    }
}

/// Credential-boundary metadata, separate from HTTP error classification.
#[derive(Debug, Clone, Copy)]
pub struct GitlabCredentialRequest<'a> {
    /// Exact approved logical/transport descriptor of this HTTP client.
    pub descriptor: &'a GitlabDescriptor,
    /// Provider-generated relative REST path, with the project encoded once.
    pub path: &'a str,
    pub writing: bool,
    // Only the provider can construct evidence for a numeric project/pipeline.
    provenance: RequestProvenance<'a>,
}

impl GitlabCredentialRequest<'_> {
    /// An ordinary path-addressed request with no corroborated numeric alias.
    #[must_use]
    pub fn direct<'a>(
        descriptor: &'a GitlabDescriptor,
        path: &'a str,
        writing: bool,
    ) -> GitlabCredentialRequest<'a> {
        GitlabCredentialRequest {
            descriptor,
            path,
            writing,
            provenance: RequestProvenance::Direct,
        }
    }
    /// Exact project and path-component match; never a host-only credential match.
    #[must_use]
    pub fn is_for_project(self, project_path: &str) -> bool {
        self.provenance
            .allows(project_path, self.path, self.writing)
    }

    /// The single native API write admitted by the current GitLab implementation.
    #[must_use]
    pub fn is_review_create(self, project_path: &str) -> bool {
        self.writing
            && self.is_for_project(project_path)
            && self.path.split('/').count() == 3
            && self.path.ends_with("/merge_requests")
    }
}

/// One injected connection scope. HTTP pools contain no persistent auth header.
pub struct GitLabSourceControl {
    client: reqwest::Client,
    api: Url,
    descriptor: GitlabDescriptor,
    credentials: Arc<dyn GitlabRequestCredentials>,
    rate_limit: RwLock<RateLimitStatus>,
}

impl GitLabSourceControl {
    /// Build an unregistered provider from an already validated descriptor.
    ///
    /// # Errors
    /// Returns a configuration error if the HTTP client cannot be constructed.
    pub fn new(
        descriptor: GitlabDescriptor,
        credentials: Arc<dyn GitlabRequestCredentials>,
    ) -> Result<Self> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .user_agent("intent-sourcecontrol")
            .build()
            .map_err(|_| Error::Config("cannot construct GitLab HTTP client".into()))?;
        Ok(Self {
            api: descriptor.api_base(),
            descriptor,
            client,
            credentials,
            rate_limit: RwLock::default(),
        })
    }

    /// Legacy aggregates cannot carry per-field availability; any consumed quota
    /// failure must propagate instead of being projected into successful data.
    async fn legacy_observation(&self, repo: &RepoRef, number: u64) -> Result<ReviewObservation> {
        let observation = self.observe_review(repo, number).await?;
        if [
            observation.availability.policy,
            observation.availability.approvals,
            observation.availability.checks,
            observation.availability.discussions,
        ]
        .contains(&ProviderAvailability::RateLimited)
        {
            return Err(Error::RateLimited(
                "GitLab optional signal rate limited".into(),
            ));
        }
        Ok(observation)
    }

    fn observe_rate_limit(&self, headers: &HeaderMap, throttled: bool) {
        let header_number = |name: &str| headers.get(name)?.to_str().ok()?.parse::<u64>().ok();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|t| t.as_secs());
        let retry_at = if throttled {
            now.and_then(|now| now.checked_add(header_number("retry-after")?))
        } else {
            None
        };
        let reset_at = header_number("ratelimit-reset").max(retry_at);
        let remaining = if throttled {
            Some(0)
        } else {
            header_number("ratelimit-remaining")
        };
        let limit = header_number("ratelimit-limit");
        if reset_at.is_some() || remaining.is_some() || limit.is_some() {
            if let Ok(mut held) = self.rate_limit.write() {
                // Another optional or concurrent response cannot shorten an active
                // rejection window. Both Retry-After and quota reset are lower bounds.
                if held.remaining == Some(0)
                    && held
                        .reset_at
                        .is_some_and(|at| now.is_some_and(|now| at > now))
                {
                    if throttled {
                        held.reset_at = held.reset_at.max(reset_at);
                        held.limit = limit.or(held.limit);
                    }
                    return;
                }
                *held = RateLimitStatus {
                    reset_at,
                    remaining,
                    limit,
                };
            }
        }
    }

    async fn page_for(
        &self,
        path: &str,
        query: Vec<(String, String)>,
        page: PageParams,
        purpose: Purpose,
    ) -> Result<Page<Value>> {
        self.page_scoped(path, query, page, purpose.into()).await
    }

    async fn page_scoped(
        &self,
        path: &str,
        mut query: Vec<(String, String)>,
        page: PageParams,
        request_scope: RequestScope<'_>,
    ) -> Result<Page<Value>> {
        let limit = page.limit.clamp(1, 100);
        let scope =
            serde_json::to_string(&(self.descriptor.instance().as_str(), path, &query, limit))?;
        let current = match page.cursor {
            None => 1,
            Some(cursor) => {
                let bytes = URL_SAFE_NO_PAD
                    .decode(cursor)
                    .map_err(|_| Error::Config("invalid GitLab page cursor".into()))?;
                let (saved, number): (String, u32) = serde_json::from_slice(&bytes)
                    .map_err(|_| Error::Config("invalid GitLab page cursor".into()))?;
                if saved != scope || !(1..=MAX_PAGES).contains(&number) {
                    return Err(Error::Config(
                        "GitLab page cursor belongs to another request".into(),
                    ));
                }
                number
            }
        };
        query.push(("page".into(), current.to_string()));
        query.push(("per_page".into(), limit.to_string()));
        let (value, headers) = self
            .request_scoped(Method::GET, path, &query, None, request_scope)
            .await?;
        let items = value
            .as_array()
            .ok_or_else(|| Error::Decode("GitLab list response is not an array".into()))?
            .clone();
        let next = if let Some(header) = headers.get("x-next-page") {
            let text = header
                .to_str()
                .map_err(|_| Error::Decode("invalid GitLab next-page header".into()))?;
            if text.is_empty() {
                None
            } else {
                Some(
                    text.parse::<u32>()
                        .map_err(|_| Error::Decode("invalid GitLab next-page header".into()))?,
                )
            }
        } else if let Some(link) = headers.get("link").and_then(|h| h.to_str().ok()) {
            let mut next = None;
            for entry in link
                .split(',')
                .filter(|entry| entry.contains("rel=\"next\"") || entry.contains("rel=next"))
            {
                let target = entry
                    .split('<')
                    .nth(1)
                    .and_then(|v| v.split('>').next())
                    .and_then(|target| Url::parse(target).ok())
                    .ok_or_else(|| Error::Decode("invalid GitLab pagination link".into()))?;
                if target.origin() != self.api.origin()
                    || !target.path().starts_with(self.api.path())
                {
                    return Err(Error::Api(
                        "GitLab pagination link escaped configured instance".into(),
                    ));
                }
                next = target
                    .query_pairs()
                    .find(|(key, _)| key == "page")
                    .and_then(|(_, value)| value.parse::<u32>().ok());
                if next.is_none() {
                    return Err(Error::Unsupported(
                        "GitLab keyset pagination on this endpoint".into(),
                    ));
                }
            }
            next
        } else if items.len() == usize::from(limit) {
            Some(current + 1)
        } else {
            None
        };
        if next.is_some_and(|next| next <= current || next > MAX_PAGES) {
            return Err(Error::Api(
                "GitLab pagination exceeded safety limit or did not advance".into(),
            ));
        }
        Ok(Page {
            items,
            next_cursor: next
                .map(|page| serde_json::to_vec(&(&scope, page)).map(|b| URL_SAFE_NO_PAD.encode(b)))
                .transpose()?,
        })
    }

    async fn page(
        &self,
        path: &str,
        query: Vec<(String, String)>,
        page: PageParams,
    ) -> Result<Page<Value>> {
        self.page_for(path, query, page, Purpose::Primary).await
    }

    async fn all_for(
        &self,
        path: &str,
        query: Vec<(String, String)>,
        purpose: Purpose,
    ) -> Result<Vec<Value>> {
        self.all_scoped(path, query, purpose.into()).await
    }

    async fn all_scoped(
        &self,
        path: &str,
        query: Vec<(String, String)>,
        scope: RequestScope<'_>,
    ) -> Result<Vec<Value>> {
        let mut output = Vec::new();
        let mut page = PageParams::first(100);
        loop {
            let result = self.page_scoped(path, query.clone(), page, scope).await?;
            output.extend(result.items);
            let Some(cursor) = result.next_cursor else {
                return Ok(output);
            };
            page = PageParams {
                limit: 100,
                cursor: Some(cursor),
            };
        }
    }
    async fn all(&self, path: &str, query: Vec<(String, String)>) -> Result<Vec<Value>> {
        self.all_for(path, query, Purpose::Primary).await
    }
}

fn encode(value: &str) -> String {
    value
        .as_bytes()
        .iter()
        .fold(String::new(), |mut out, byte| {
            use std::fmt::Write as _;
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
                out.push(char::from(*byte));
            } else {
                let _ = write!(out, "%{byte:02X}");
            }
            out
        })
}
fn project(repo: &RepoRef) -> String {
    format!(
        "projects/{}",
        encode(&format!("{}/{}", repo.owner, repo.name))
    )
}
fn mr(repo: &RepoRef, number: u64) -> String {
    format!("{}/merge_requests/{number}", project(repo))
}
fn string(value: &Value, key: &str) -> Result<String> {
    value[key]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| Error::Decode(format!("GitLab response missing {key}")))
}
fn optional(value: &Value, key: &str) -> Option<String> {
    value[key].as_str().map(str::to_owned)
}
fn number(value: &Value, key: &str) -> Result<u64> {
    value[key]
        .as_u64()
        .ok_or_else(|| Error::Decode(format!("GitLab response missing {key}")))
}
fn array<'a>(value: &'a Value, key: &str) -> Result<&'a Vec<Value>> {
    value[key]
        .as_array()
        .ok_or_else(|| Error::Decode(format!("GitLab response missing {key}")))
}
fn author(value: &Value) -> String {
    value["author"]["username"]
        .as_str()
        .unwrap_or("ghost")
        .into()
}
fn map_page<T>(page: Page<Value>, map: impl Fn(Value) -> Result<T>) -> Result<Page<T>> {
    Ok(Page {
        items: page.items.into_iter().map(map).collect::<Result<_>>()?,
        next_cursor: page.next_cursor,
    })
}
fn mergeable(status: &str) -> Option<bool> {
    match status {
        "mergeable" => Some(true),
        "conflict"
        | "need_rebase"
        | "not_approved"
        | "requested_changes"
        | "ci_must_pass"
        | "ci_still_running"
        | "discussions_not_resolved"
        | "draft_status"
        | "blocked_status"
        | "not_open"
        | "broken_status"
        | "external_status_checks"
        | "locked_paths" => Some(false),
        _ => None,
    }
}
fn normalized_merge_status(status: &str) -> String {
    match status {
        "mergeable" => "clean",
        "conflict" => "dirty",
        "need_rebase" => "behind",
        "ci_still_running" | "ci_must_pass" => "unstable",
        "not_approved"
        | "requested_changes"
        | "discussions_not_resolved"
        | "draft_status"
        | "blocked_status"
        | "not_open"
        | "broken_status"
        | "external_status_checks"
        | "locked_paths" => "blocked",
        _ => "unknown",
    }
    .into()
}

// Owned callback shared by direct responses and map_page.
#[expect(clippy::needless_pass_by_value)]
fn to_pr(value: Value) -> Result<PullRequest> {
    let status = optional(&value, "detailed_merge_status");
    let state = match value["state"].as_str() {
        Some("opened" | "locked") => PrState::Open,
        Some("merged") => PrState::Merged,
        Some("closed") => PrState::Closed,
        _ => return Err(Error::Decode("unknown GitLab merge request state".into())),
    };
    Ok(PullRequest {
        number: number(&value, "iid")?,
        url: string(&value, "web_url")?,
        title: string(&value, "title")?,
        body: optional(&value, "description"),
        state,
        draft: value["draft"].as_bool().unwrap_or(false),
        source_branch: optional(&value, "source_branch").unwrap_or_default(),
        target_branch: optional(&value, "target_branch").unwrap_or_default(),
        author: author(&value),
        mergeable: status.as_deref().and_then(mergeable),
        mergeable_state: status.as_deref().map(normalized_merge_status),
        head_sha: optional(&value, "sha"),
        created_at: string(&value, "created_at")?,
        updated_at: string(&value, "updated_at")?,
    })
}
// Owned callback shared by direct responses and map_page.
#[expect(clippy::needless_pass_by_value)]
fn to_repo(value: Value) -> Result<Repo> {
    let path = string(&value, "path_with_namespace")?;
    let (owner, name) = path
        .rsplit_once('/')
        .ok_or_else(|| Error::Decode("GitLab project has no namespace".into()))?;
    Ok(Repo {
        owner: owner.into(),
        name: name.into(),
        url: optional(&value, "web_url"),
        default_branch: optional(&value, "default_branch"),
        created_at: optional(&value, "created_at"),
        updated_at: optional(&value, "last_activity_at"),
    })
}
// Owned callback shared by direct responses and map_page.
#[expect(clippy::needless_pass_by_value)]
fn to_issue(value: Value) -> Result<Issue> {
    Ok(Issue {
        number: number(&value, "iid")?,
        title: string(&value, "title")?,
        body: optional(&value, "description"),
        state: match value["state"].as_str() {
            Some("opened") => "open".into(),
            Some("closed") => "closed".into(),
            _ => return Err(Error::Decode("unknown GitLab issue state".into())),
        },
        url: string(&value, "web_url")?,
        author: author(&value),
        created_at: string(&value, "created_at")?,
        updated_at: string(&value, "updated_at")?,
    })
}
fn to_comment(value: &Value) -> Result<Comment> {
    Ok(Comment {
        id: number(value, "id")?.to_string(),
        author: author(value),
        body: string(value, "body")?,
        path: optional(&value["position"], "new_path")
            .or_else(|| optional(&value["position"], "old_path")),
        line: value["position"]["new_line"]
            .as_u64()
            .or_else(|| value["position"]["old_line"].as_u64()),
        created_at: string(value, "created_at")?,
        url: optional(value, "url"),
    })
}
fn to_review_comment(value: &Value, reply_to: Option<u64>) -> Result<ReviewComment> {
    let comment = to_comment(value)?;
    Ok(ReviewComment {
        id: number(value, "id")?,
        body: comment.body,
        path: comment.path.unwrap_or_default(),
        line: comment.line,
        author: comment.author,
        created_at: comment.created_at,
        updated_at: string(value, "updated_at")?,
        in_reply_to_id: reply_to,
        url: comment.url.unwrap_or_default(),
    })
}
fn encode_thread(instance: &str, repo: &RepoRef, iid: u64, id: &str) -> String {
    format!(
        "gitlab:{}",
        URL_SAFE_NO_PAD.encode(
            json!([instance, format!("{}/{}", repo.owner, repo.name), iid, id]).to_string()
        )
    )
}
fn state(value: &str) -> CheckState {
    match value {
        "success" => CheckState::Success,
        "failed" => CheckState::Failure,
        "canceled" => CheckState::Cancelled,
        "skipped" => CheckState::Neutral,
        _ => CheckState::Pending,
    }
}
fn known_check_status(value: &Value) -> bool {
    matches!(
        value.as_str(),
        Some(
            "created"
                | "waiting_for_resource"
                | "preparing"
                | "pending"
                | "running"
                | "success"
                | "failed"
                | "canceled"
                | "skipped"
                | "manual"
                | "scheduled"
        )
    )
}
fn job_state(job: &Value) -> Result<CheckState> {
    let status = string(job, "status")?;
    Ok(
        if job["allow_failure"] == true
            && matches!(status.as_str(), "failed" | "manual" | "canceled")
        {
            CheckState::Neutral
        } else {
            state(&status)
        },
    )
}

fn pipeline_check(pipeline: &Value, policy: Option<&Value>) -> RollupCheck {
    let required = policy.is_some_and(|p| p["only_allow_merge_if_pipeline_succeeds"] == true);
    let status = pipeline["status"].as_str().unwrap_or_default();
    let state = if status == "skipped" && required {
        if policy.is_some_and(|p| p["allow_merge_on_skipped_pipeline"] == true) {
            CheckState::Success
        } else {
            CheckState::Failure
        }
    } else {
        state(status)
    };
    RollupCheck {
        name: PIPELINE_CHECK.into(),
        kind: RollupCheckKind::CheckRun,
        state,
        is_required: required,
        url: optional(pipeline, "web_url"),
        started_at: optional(pipeline, "started_at").or_else(|| optional(pipeline, "created_at")),
    }
}

fn project_rules(policy: &Value) -> BranchRules {
    BranchRules {
        // Branch-only reads cannot know which code-owner/MR override rules apply.
        required_approving_review_count: None,
        required_conversation_resolution: policy
            ["only_allow_merge_if_all_discussions_are_resolved"]
            .as_bool(),
        required_status_checks: if policy["only_allow_merge_if_pipeline_succeeds"] == true {
            vec![PIPELINE_CHECK.into()]
        } else {
            vec![]
        },
    }
}

fn approval_decision(value: &Value) -> Option<ReviewDecision> {
    // `approved` is authoritative for effective rules in EE. CE has no required
    // rules and reports false until somebody voluntarily approves the MR.
    if value["approved"] == true {
        Some(ReviewDecision::Approved)
    } else if value["approvals_left"]
        .as_u64()
        .is_some_and(|left| left > 0)
        || value["approvals_required"]
            .as_u64()
            .is_some_and(|required| required > 0)
    {
        Some(ReviewDecision::ReviewRequired)
    } else {
        None
    }
}

fn approval_reviews(value: &Value) -> Result<Vec<Review>> {
    array(value, "approved_by")?
        .iter()
        .map(|entry| {
            Ok(Review {
                author: string(&entry["user"], "username")?,
                verdict: ReviewVerdict::Approve,
                body: None,
                submitted_at: optional(entry, "approved_at").unwrap_or_default(),
            })
        })
        .collect()
}

fn discussion_tally(discussions: &[Value]) -> Result<(ReviewThreadTally, i64)> {
    let mut tally = ReviewThreadTally::default();
    let mut conversation_count = 0;
    for discussion in discussions {
        let notes = array(discussion, "notes")?;
        if notes.iter().any(|note| {
            !note["system"].is_boolean()
                || !note["resolvable"].is_boolean()
                || note["resolvable"] == true && !note["resolved"].is_boolean()
        }) {
            return Err(Error::Decode("GitLab discussion status is unknown".into()));
        }
        let review_thread = notes.iter().any(|note| note["resolvable"] == true);
        let count = i64::try_from(notes.iter().filter(|note| note["system"] != true).count())
            .map_err(|_| Error::Decode("too many GitLab discussion notes".into()))?;
        if review_thread {
            tally.review_comment_count += count;
            if notes
                .iter()
                .any(|note| note["resolvable"] == true && note["resolved"] != true)
            {
                tally.unresolved += 1;
            }
        } else {
            conversation_count += count;
        }
    }
    Ok((tally, conversation_count))
}

#[async_trait]
impl SourceControl for GitLabSourceControl {
    fn provider_id(&self) -> &'static str {
        "gitlab"
    }

    fn capabilities(&self) -> ScCapabilities {
        ScCapabilities {
            draft_prs: false,
            squash_merge: false,
            rebase_merge: false,
            review_required_changes: false,
            check_runs: true,
            issues: true,
        }
    }
    async fn rate_limit_status(&self) -> Result<RateLimitStatus> {
        Ok(self
            .rate_limit
            .read()
            .map(|status| *status)
            .unwrap_or_default())
    }

    async fn check_auth(&self) -> Result<AuthStatus> {
        let user = self.get_user().await?;
        Ok(AuthStatus {
            authenticated: true,
            login: Some(user.login),
            scopes: vec![],
        })
    }

    async fn get_user(&self) -> Result<UserIdentity> {
        let value = self.get("user").await?;
        Ok(UserIdentity {
            login: string(&value, "username")?,
            id: value["id"].as_u64(),
            name: optional(&value, "name"),
            avatar_url: optional(&value, "avatar_url"),
            html_url: optional(&value, "web_url"),
        })
    }

    async fn list_repos(&self, page: PageParams) -> Result<Page<Repo>> {
        map_page(
            self.page(
                "projects",
                vec![
                    ("membership".into(), "true".into()),
                    ("order_by".into(), "last_activity_at".into()),
                    // The picker only needs project summaries, not per-project policy details.
                    ("simple".into(), "true".into()),
                ],
                page,
            )
            .await?,
            to_repo,
        )
    }

    async fn search_repos(&self, query: &str, page: PageParams) -> Result<Page<Repo>> {
        map_page(
            self.page(
                "projects",
                vec![
                    ("search".into(), query.into()),
                    ("search_namespaces".into(), "true".into()),
                    ("simple".into(), "true".into()),
                ],
                page,
            )
            .await?,
            to_repo,
        )
    }

    async fn get_repo(&self, owner: &str, name: &str) -> Result<Repo> {
        to_repo(self.get_project(&RepoRef::new(owner, name)).await?)
    }

    async fn list_remote_branches(
        &self,
        owner: &str,
        name: &str,
        prefix: Option<&str>,
        page: PageParams,
    ) -> Result<Page<Branch>> {
        let query = prefix
            .filter(|v| !v.is_empty())
            .map(|v| vec![("search".into(), format!("^{v}"))])
            .unwrap_or_default();
        map_page(
            self.page(
                &format!(
                    "{}/repository/branches",
                    project(&RepoRef::new(owner, name))
                ),
                query,
                page,
            )
            .await?,
            |value| {
                Ok(Branch {
                    name: string(&value, "name")?,
                    commit_sha: optional(&value["commit"], "id"),
                    protected: value["protected"].as_bool().unwrap_or(false),
                })
            },
        )
    }

    async fn get_file_content(
        &self,
        repo: &RepoRef,
        path: &str,
        git_ref: Option<&str>,
    ) -> Result<Option<String>> {
        let response = self
            .request(
                Method::GET,
                &format!("{}/repository/files/{}", project(repo), encode(path)),
                &[("ref".into(), git_ref.unwrap_or("HEAD").into())],
                None,
            )
            .await;
        let value = match response {
            Ok((value, _)) => value,
            Err(error) => return Err(error),
        };
        if value["encoding"] != "base64" {
            return Err(Error::Decode("unsupported GitLab file encoding".into()));
        }
        let encoded = string(&value, "content")?.replace(['\n', '\r'], "");
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|_| Error::Decode("invalid GitLab file content".into()))?;
        String::from_utf8(bytes)
            .map(Some)
            .map_err(|_| Error::Decode("GitLab file is not UTF-8".into()))
    }

    async fn get_pr(&self, repo: &RepoRef, number: u64) -> Result<PullRequest> {
        Ok(self.review_details(repo, number).await?.review)
    }

    async fn list_prs(&self, repo: &RepoRef, query: PrQuery) -> Result<Page<PullRequest>> {
        let mut params = vec![("scope".into(), "all".into())];
        if let Some(state) = query.state {
            params.push((
                "state".into(),
                match state {
                    PrState::Open => "opened",
                    PrState::Closed => "closed",
                    PrState::Merged => "merged",
                }
                .into(),
            ));
        }
        for (key, value) in [
            ("source_branch", query.head),
            ("target_branch", query.base),
            ("author_username", query.author),
            ("search", query.search),
        ] {
            if let Some(value) = value {
                params.push((key.into(), value));
            }
        }
        let mut involves = None;
        if let Some(involvement) = query.involvement {
            let user = self.get_user().await?;
            let key = match involvement {
                PrInvolvement::Created => "author_username",
                PrInvolvement::Assigned => "assignee_username[]",
                PrInvolvement::ReviewRequested => "reviewer_username",
                PrInvolvement::Involves => {
                    involves = Some(user.login.clone());
                    ""
                }
            };
            if !key.is_empty() {
                params.push((key.into(), user.login));
            }
        }
        let page = PageParams {
            limit: query.limit.unwrap_or(30),
            cursor: query.cursor,
        };
        let result = if !query.extra_repos.is_empty() || involves.is_some() {
            self.search_projects(
                repo,
                &query.extra_repos,
                "merge_requests",
                params,
                page,
                involves.as_deref(),
            )
            .await?
        } else {
            self.page(&format!("{}/merge_requests", project(repo)), params, page)
                .await?
        };
        map_page(result, |value| Ok(self.details(value)?.review))
    }

    async fn list_comments(&self, repo: &RepoRef, number: u64) -> Result<Vec<Comment>> {
        self.all(&format!("{}/notes", mr(repo, number)), vec![])
            .await?
            .iter()
            .filter(|note| note["system"] != true && note["type"] != "DiffNote")
            .map(to_comment)
            .collect()
    }

    async fn list_review_comments(
        &self,
        repo: &RepoRef,
        number: u64,
        page: PageParams,
    ) -> Result<Page<ReviewComment>> {
        let page = self
            .page(&format!("{}/discussions", mr(repo, number)), vec![], page)
            .await?;
        let mut items = Vec::new();
        for thread in page.items {
            let notes = array(&thread, "notes")?;
            if notes.first().is_some_and(|note| note["type"] == "DiffNote") {
                let first = notes.first().and_then(|note| note["id"].as_u64());
                for (index, note) in notes.iter().enumerate() {
                    items.push(to_review_comment(
                        note,
                        if index == 0 { None } else { first },
                    )?);
                }
            }
        }
        Ok(Page {
            items,
            next_cursor: page.next_cursor,
        })
    }

    async fn get_review_threads(
        &self,
        repo: &RepoRef,
        number: u64,
        page: PageParams,
    ) -> Result<Page<ReviewThread>> {
        let page = self
            .page(&format!("{}/discussions", mr(repo, number)), vec![], page)
            .await?;
        let mut items = Vec::new();
        for thread in page.items {
            let notes = array(&thread, "notes")?;
            if !notes.iter().any(|note| note["resolvable"] == true) {
                continue;
            }
            let root = notes
                .first()
                .ok_or_else(|| Error::Decode("GitLab returned an empty discussion".into()))?;
            let root_comment = to_comment(root)?;
            let comments = notes
                .iter()
                .filter(|note| note["system"] != true)
                .map(|note| {
                    let comment = to_comment(note)?;
                    Ok(ReviewThreadComment {
                        id: comment.id,
                        body: comment.body,
                        author: comment.author,
                        path: comment
                            .path
                            .or_else(|| root_comment.path.clone())
                            .unwrap_or_default(),
                        line: comment.line.or(root_comment.line),
                        created_at: comment.created_at,
                    })
                })
                .collect::<Result<_>>()?;
            items.push(ReviewThread {
                id: encode_thread(
                    self.descriptor.instance().as_str(),
                    repo,
                    number,
                    &string(&thread, "id")?,
                ),
                is_resolved: notes
                    .iter()
                    .filter(|note| note["resolvable"] == true)
                    .all(|note| note["resolved"] == true),
                comments,
            });
        }
        Ok(Page {
            items,
            next_cursor: page.next_cursor,
        })
    }

    async fn check_runs(&self, repo: &RepoRef, git_ref: &str) -> Result<Vec<CheckRun>> {
        let commit = self
            .get(&format!(
                "{}/repository/commits/{}",
                project(repo),
                encode(git_ref)
            ))
            .await?;
        let sha = string(&commit, "id")?;
        let statuses = self
            .all(
                &format!(
                    "{}/repository/commits/{}/statuses",
                    project(repo),
                    encode(&sha)
                ),
                vec![],
            )
            .await?;
        let mut checks = statuses
            .iter()
            .map(|value| {
                Ok(CheckRun {
                    name: string(value, "name")?,
                    state: state(&string(value, "status")?),
                    url: optional(value, "target_url"),
                    started_at: optional(value, "started_at"),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let pipelines = self
            .page(
                &format!("{}/pipelines", project(repo)),
                vec![
                    ("sha".into(), sha),
                    ("order_by".into(), "id".into()),
                    ("sort".into(), "desc".into()),
                ],
                PageParams::first(1),
            )
            .await?;
        if let Some(pipeline) = pipelines.items.first() {
            let id = number(pipeline, "id")?;
            checks.push(CheckRun {
                name: "GitLab pipeline".into(),
                state: state(&string(pipeline, "status")?),
                url: optional(pipeline, "web_url"),
                started_at: optional(pipeline, "created_at"),
            });
            let jobs = self
                .all(&format!("{}/pipelines/{id}/jobs", project(repo)), vec![])
                .await?;
            for job in jobs {
                checks.push(CheckRun {
                    name: string(&job, "name")?,
                    state: job_state(&job)?,
                    url: optional(&job, "web_url"),
                    started_at: optional(&job, "started_at"),
                });
            }
        }
        Ok(checks)
    }

    async fn get_issue(&self, repo: &RepoRef, number: u64) -> Result<Issue> {
        to_issue(
            self.get(&format!("{}/issues/{number}", project(repo)))
                .await?,
        )
    }

    async fn list_issues(&self, repo: &RepoRef, query: IssueQuery) -> Result<Page<Issue>> {
        let mut params = vec![("scope".into(), "all".into())];
        if let Some(state) = query.state {
            params.push((
                "state".into(),
                if state == "open" {
                    "opened".into()
                } else {
                    state
                },
            ));
        }
        if let Some(labels) = query.labels {
            params.push(("labels".into(), labels));
        }
        if let Some(search) = query.search {
            params.push(("search".into(), search));
        }
        let page = PageParams {
            limit: query.limit.unwrap_or(30),
            cursor: query.cursor,
        };
        let result = if query.extra_repos.is_empty() {
            self.page(&format!("{}/issues", project(repo)), params, page)
                .await?
        } else {
            self.search_projects(repo, &query.extra_repos, "issues", params, page, None)
                .await?
        };
        map_page(result, to_issue)
    }
    async fn create_pr(&self, repo: &RepoRef, input: NewPullRequest) -> Result<PullRequest> {
        self.create_legacy(repo, input).await
    }
    async fn review_details(&self, repo: &RepoRef, number: u64) -> Result<ReviewDetails> {
        self.details(self.get(&mr(repo, number)).await?)
    }
    async fn review_observation(&self, repo: &RepoRef, number: u64) -> Result<ReviewObservation> {
        self.observe_review(repo, number).await
    }
    async fn list_reviews(&self, repo: &RepoRef, number: u64) -> Result<Vec<Review>> {
        let (value, _) = self
            .request_for(
                Method::GET,
                &format!("{}/approvals", mr(repo, number)),
                &[],
                None,
                Purpose::Optional,
            )
            .await?;
        approval_reviews(&value)
    }
    async fn review_decision(&self, repo: &RepoRef, number: u64) -> Result<Option<ReviewDecision>> {
        Ok(self
            .legacy_observation(repo, number)
            .await?
            .signals
            .review_decision)
    }
    async fn branch_rules(&self, repo: &RepoRef, _branch: &str) -> Result<BranchRules> {
        let value = self.get_project(repo).await?;
        Ok(project_rules(&value))
    }
    async fn merge_requirements(
        &self,
        repo: &RepoRef,
        number: u64,
    ) -> Result<MergeRequirementSignals> {
        let observation = self.legacy_observation(repo, number).await?;
        Ok(observation.signals)
    }
    async fn pr_observation(&self, repo: &RepoRef, number: u64) -> Result<Option<PrObservation>> {
        let o = self.legacy_observation(repo, number).await?;
        // The legacy count is not nullable, so an unknown count must fail this projection.
        let count = o
            .conversation_count
            .ok_or_else(|| failure(ProviderFailureKind::Unknown, None))?;
        Ok(Some(PrObservation {
            pr: o.details.review,
            signals: o.signals,
            reviews: o.reviews,
            threads: o.threads,
            conversation_count: count,
        }))
    }
    async fn mergeability(&self, repo: &RepoRef, number: u64) -> Result<Mergeability> {
        let observation = self.legacy_observation(repo, number).await?;
        let signals = &observation.signals;
        Ok(Mergeability {
            mergeable: observation.details.review.mergeable,
            conflicts: signals.merge_state_status.as_deref() == Some("conflict"),
            required_checks_passed: signals.checks_known
                && signals
                    .checks
                    .iter()
                    .filter(|c| c.is_required)
                    .all(|c| c.state == CheckState::Success),
        })
    }
    async fn update_pr(
        &self,
        _repo: &RepoRef,
        _number: u64,
        _patch: PrPatch,
    ) -> Result<PullRequest> {
        unsupported_write()
    }
    async fn merge_pr(
        &self,
        _repo: &RepoRef,
        _number: u64,
        _method: MergeMethod,
        _opts: MergeOptions,
    ) -> Result<MergeOutcome> {
        unsupported_write()
    }
    async fn update_branch(&self, _repo: &RepoRef, _number: u64) -> Result<()> {
        unsupported_write()
    }
    async fn submit_review(
        &self,
        _repo: &RepoRef,
        _number: u64,
        _verdict: ReviewVerdict,
        _body: Option<String>,
    ) -> Result<Review> {
        unsupported_write()
    }
    async fn add_comment(
        &self,
        _repo: &RepoRef,
        _number: u64,
        _body: &str,
        _anchor: Option<CommentAnchor>,
    ) -> Result<Comment> {
        unsupported_write()
    }
    async fn reply_to_review_comment(
        &self,
        _repo: &RepoRef,
        _number: u64,
        _comment_id: u64,
        _body: &str,
    ) -> Result<ReviewComment> {
        unsupported_write()
    }
    async fn resolve_thread(&self, _thread_id: &str) -> Result<bool> {
        unsupported_write()
    }
    async fn unresolve_thread(&self, _thread_id: &str) -> Result<bool> {
        unsupported_write()
    }
    async fn create_issue(
        &self,
        _repo: &RepoRef,
        _title: &str,
        _body: Option<&str>,
    ) -> Result<Issue> {
        unsupported_write()
    }
}

fn unsupported_write<T>() -> Result<T> {
    Err(Error::Unsupported(
        "GitLab write operation outside same-project creation".into(),
    ))
}
