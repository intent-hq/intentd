//! A scripted `SourceControl` for PR tests: each flag or hook makes one
//! forge call fail, park or answer a canned value.

use async_trait::async_trait;
use intent_sourcecontrol::{
    AuthStatus, Branch, CheckRun, CheckState, Comment, CommentAnchor,
    Error as ScError, Issue, IssueQuery, MergeMethod, MergeOptions, MergeOutcome,
    MergeRequirementSignals, Mergeability, NewPullRequest, Page, PageParams, PrPatch, PrQuery,
    PrState, PullRequest, RateLimitStatus, Repo, RepoRef, Result as ScResult, Review,
    ReviewComment, ReviewDecision, ReviewThread, ReviewThreadComment, ReviewVerdict,
    ScCapabilities, SourceControl, UserIdentity,
};

// Test stub: one independent bool per scripted scenario.
#[expect(clippy::struct_excessive_bools)]
#[derive(Default)]
pub struct StubForge {
    pub fail_threads: bool,
    /// When set, `get_review_threads` fails with `RateLimited` (the
    /// GraphQL quota is exhausted), exercising the checklist's
    /// quota-exhaustion propagation instead of the REST fallback.
    pub rate_limited_threads: bool,
    /// When set, `list_review_comments` fails too, so a `fail_threads`
    /// checklist has no comment source at all.
    pub fail_review_comments: bool,
    /// When set, `list_prs` returns the sample PR so PR-refresh discovery can
    /// link an unlinked workspace by head ref.
    pub discover: bool,
    /// When set, `get_pr` returns the sample PR as merged, so refresh sees a
    /// linked PR whose lifecycle ended (relink-after-merge tests).
    pub merged_linked: bool,
    /// When set, `list_prs` also returns an *open* PR with this number (same
    /// head ref as the sample), simulating a newer PR on the same branch.
    pub open_pr_number: Option<u64>,
    /// When set, `list_prs` fails, simulating a transient forge error
    /// during relink discovery.
    pub fail_list_prs: bool,
    /// When set, `list_repos` emits a two-page sequence driven by the opaque
    /// cursor, exercising the §5.5 multi-page round-trip end to end.
    pub paginate: bool,
    /// When >0, `get_review_threads` emits this many single-thread pages
    /// driven by a numeric cursor (unresolved thread `RT{n}` on page `n`),
    /// exercising the exhaustive `pr.listReviewComments` fetch.
    pub thread_pages: u64,
    /// When >0, `list_review_comments` emits this many single-comment pages
    /// driven by a numeric cursor (comment id `n` on page `n`, no reply
    /// parent), exercising the multi-page REST fallback.
    pub review_comment_pages: u64,
    /// When set, overrides the advertised [`ScCapabilities`] (default is
    /// all-true), exercising the `pr.capabilities` surface + runtime
    /// gating of `pr.merge` / `pr.createReview` / `pr.listCheckRuns`.
    pub capabilities: Option<ScCapabilities>,
    /// What `get_file_content` returns for any path (`None` → the file is
    /// absent at that ref), exercising `github.repoConfig.get`.
    pub file_content: Option<String>,
    /// When true, `get_file_content` fails with a decode error (mis-shaped
    /// contents payload: directory, non-base64/non-UTF-8), exercising the
    /// tolerant fold of `github.repoConfig.get`.
    pub file_content_decode_error: bool,
    /// When set, `get_pr` returns `NotFound` for this PR number,
    /// exercising the `pr_state` nonexistent-PR error path.
    pub missing_pr: Option<u64>,
    /// When true, `get_pr` reports the sample PR as unmergeable with
    /// conflicts (`mergeable: false`, `mergeableState: "dirty"`),
    /// exercising the `pr_state` `mergeBlockedReason` wiring.
    pub dirty_pr: bool,
    /// When true, `get_pr` reports the sample PR with
    /// `mergeableState: "blocked"` (required checks / merge queue /
    /// token without push access), exercising the `pr_state` decision
    /// regression (intent-hq/monorepo#1524).
    pub blocked_pr: bool,
    /// When true, `list_reviews` returns no reviews at all.
    pub no_reviews: bool,
    /// The forge's authoritative `reviewDecision` (`None` mirrors an
    /// unprotected base / host without the signal).
    pub review_decision: Option<ReviewDecision>,
    /// When true, `review_decision` fails, exercising the degrade-to-
    /// aggregate path of `pr_state`.
    pub fail_review_decision: bool,
    /// What the merge-requirements probe reports. `None` mirrors a host
    /// without the signals (the trait default `Unsupported`), exercising
    /// the degraded checklist path.
    pub merge_signals: Option<MergeRequirementSignals>,
    /// PR number whose `get_pr` pends forever (hung-connection
    /// regression, intent-hq/monorepo#1988 lineage).
    pub hang_get_pr: Option<u64>,
    /// PR numbers `get_pr` was called with, in call order (sweep
    /// stale-pool heal tests assert cap + ordering).
    pub seen_get_pr: std::sync::Mutex<Vec<u64>>,
    /// Issue numbers `get_issue` was called with, in call order (the
    /// issue cache tests assert a hit costs no forge read).
    pub seen_get_issue: std::sync::Mutex<Vec<u64>>,
    /// Issue number whose `get_issue` fails with `NotFound`, exercising
    /// the issue cache's never-cache-errors rule.
    pub missing_issue: Option<u64>,
    /// When true, `get_pr` and `list_prs` fail with `RateLimited`
    /// (exhausted GitHub core quota, monorepo#2961), exercising the
    /// global sweep pause.
    pub rate_limited: bool,
    /// When true, only `list_prs` fails with `RateLimited` (the linked
    /// `get_pr` still succeeds), exercising the merged/closed relink
    /// discovery hitting the quota mid-refresh (monorepo#2961).
    pub rate_limited_list_prs: bool,
    /// The reset `rate_limit_status` reports (`None` mirrors a host
    /// without the signal → fallback pause).
    pub rate_limit_reset: Option<u64>,
    /// The `remaining` / `limit` `rate_limit_status` reports (`None`
    /// mirrors a host without the signal → the pause runs its window).
    pub rate_limit_remaining: Option<u64>,
    pub rate_limit_limit: Option<u64>,
    /// How many times `rate_limit_status` was probed.
    pub seen_reset_probes: std::sync::Mutex<u64>,
    /// Every [`PrQuery`] handed to `list_prs`, in call order (the
    /// `github.pulls.search` `repos` tests assert `extra_repos`).
    pub seen_pr_queries: std::sync::Mutex<Vec<PrQuery>>,
    /// Every [`IssueQuery`] handed to `list_issues`, in call order.
    pub seen_issue_queries: std::sync::Mutex<Vec<IssueQuery>>,
    /// Runs at the start of every `list_comments` (the last forge read
    /// of `pr_state`), so a test can move daemon state mid-snapshot.
    pub on_list_comments: std::sync::Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    /// Every `(query, limit)` handed to `search_users`, in call order
    /// (the `github.users.search` tests assert trimming, the clamp and
    /// the blank-query short-circuit).
    pub seen_user_searches: std::sync::Mutex<Vec<(String, u8)>>,
    /// When true, `check_auth` reports no working credential (a revoked
    /// or missing token), exercising the invite-create live auth gate
    /// (multiplayer w4).
    pub unauthenticated: bool,
    /// Accounts `get_user_by_login` knows (keyed by lowercase login);
    /// any other login is `NotFound`. Empty → the trait default
    /// (`Unsupported`), so unrelated tests keep their behaviour.
    pub users_by_login: std::collections::HashMap<String, UserIdentity>,
    /// What `get_proof_gist` answers per gist id: `Ok(view)` or the
    /// scripted error (`Err("not-found")` → `NotFound`,
    /// `Err("anonymous")` → `Decode` — a gist without `owner.login` /
    /// `created_at` — anything else → `Api`). An unknown id is
    /// `NotFound`. Exercises `invite.prove`.
    pub proof_gists: std::collections::HashMap<
        String,
        std::result::Result<intent_sourcecontrol::identity_proof::ProofGistView, String>,
    >,
    /// Every gist id handed to `get_proof_gist`, in call order.
    pub seen_proof_gists: std::sync::Mutex<Vec<String>>,
    /// How many times `get_user` was called (the primary identity
    /// refresh tests assert exactly one spawn per interval). Counted
    /// before [`Self::get_user_gate`] is awaited, so a held call is
    /// still visible.
    pub get_user_calls: std::sync::atomic::AtomicU64,
    /// How many times `check_auth` was called (the unauthenticated
    /// refresh tests wait on the auth-gate probe, then assert no
    /// `get_user` followed).
    pub check_auth_calls: std::sync::atomic::AtomicU64,
    /// When set, `get_user` holds until the test releases a permit
    /// (`add_permits(1)`): the caller proves the read it triggered from
    /// returned while `GET /user` was still in flight.
    pub get_user_gate: Option<std::sync::Arc<tokio::sync::Semaphore>>,
    /// When set, the FIRST `get_pr` signals `entered` and holds until
    /// `release` is notified, so a test can land a concurrent
    /// signal-bearing fold while a REST refresh's read is in flight
    /// (intent-hq/intent#5654).
    pub get_pr_park: Option<std::sync::Arc<GetPrPark>>,
    pub get_pr_error: Option<fn() -> ScError>,
    pub create_pr_error: Option<fn() -> ScError>,
    pub create_pr_calls: std::sync::atomic::AtomicU64,
}

/// One-shot park for [`StubForge::get_pr`]: `entered` fires when the
/// held read begins, `release` lets it return.
#[derive(Default)]
pub struct GetPrPark {
    claimed: std::sync::atomic::AtomicBool,
    pub entered: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
}

impl StubForge {
    pub fn with_get_pr_error(error: fn() -> ScError) -> Self {
        Self {
            get_pr_error: Some(error),
            ..Default::default()
        }
    }

    /// A forge whose `check_auth` reports `authenticated: false` and
    /// whose `get_user` rejects the credential (`Auth`), as the real
    /// client does on a 401.
    pub fn unauthenticated() -> Self {
        Self {
            unauthenticated: true,
            ..Default::default()
        }
    }

    /// A forge whose `get_user` holds until `gate` has a permit.
    pub fn with_get_user_gate(gate: std::sync::Arc<tokio::sync::Semaphore>) -> Self {
        Self {
            get_user_gate: Some(gate),
            ..Default::default()
        }
    }
}

/// The blended multi-repo page a forge answers for a search whose
/// `extra_repos` is non-empty: one hit per scoped repo (the addressed
/// `repo` first, then the extras), each hit's URL naming ITS OWN repo,
/// newest-updated first, with a continuation cursor.
pub fn multi_repo_hits(repo: &RepoRef, extra_repos: &[RepoRef]) -> Vec<(u64, String, String)> {
    std::iter::once(repo)
        .chain(extra_repos.iter())
        .enumerate()
        .map(|(i, r)| {
            let number = 100 + i as u64;
            (
                number,
                format!("https://github.com/{}/{}", r.owner, r.name),
                format!("2026-01-0{}T00:00:00Z", 9 - i),
            )
        })
        .collect()
}

pub fn sample_pr() -> PullRequest {
    PullRequest {
        number: 42,
        url: "https://github.com/o/r/pull/42".into(),
        title: "Add thing".into(),
        body: None,
        state: PrState::Open,
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
        self.capabilities.unwrap_or(ScCapabilities {
            draft_prs: true,
            squash_merge: true,
            rebase_merge: true,
            review_required_changes: true,
            check_runs: true,
            issues: true,
        })
    }
    async fn check_auth(&self) -> ScResult<AuthStatus> {
        self.check_auth_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.unauthenticated {
            return Ok(AuthStatus {
                authenticated: false,
                login: None,
                scopes: vec![],
            });
        }
        Ok(AuthStatus {
            authenticated: true,
            login: Some("octocat".into()),
            scopes: vec![],
        })
    }
    async fn get_user(&self) -> ScResult<UserIdentity> {
        self.get_user_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if let Some(gate) = &self.get_user_gate {
            gate.acquire().await.expect("gate open").forget();
        }
        if self.unauthenticated {
            return Err(ScError::Auth("Bad credentials".into()));
        }
        Ok(UserIdentity {
            login: "octocat".into(),
            id: Some(583_231),
            name: Some("The Octocat".into()),
            avatar_url: Some("https://avatars.example/u/1".into()),
            html_url: Some("https://github.com/octocat".into()),
        })
    }
    async fn get_user_by_login(&self, login: &str) -> ScResult<UserIdentity> {
        if self.users_by_login.is_empty() {
            return Err(ScError::Unsupported("no scripted accounts".into()));
        }
        self.users_by_login
            .get(&login.to_ascii_lowercase())
            .cloned()
            .ok_or_else(|| ScError::NotFound(format!("github user {login:?}")))
    }
    async fn get_proof_gist(
        &self,
        gist_id: &str,
    ) -> ScResult<intent_sourcecontrol::identity_proof::ProofGistView> {
        self.seen_proof_gists
            .lock()
            .unwrap()
            .push(gist_id.to_string());
        match self.proof_gists.get(gist_id) {
            Some(Ok(view)) => Ok(view.clone()),
            Some(Err(kind)) if kind == "not-found" => {
                Err(ScError::NotFound(format!("gist {gist_id:?}")))
            }
            Some(Err(kind)) if kind == "anonymous" => Err(ScError::Decode(
                "GET /gists/{id} response missing `owner.login`".to_string(),
            )),
            Some(Err(kind)) => Err(ScError::Api(format!("502: {kind}"))),
            None => Err(ScError::NotFound(format!("gist {gist_id:?}"))),
        }
    }
    async fn search_users(&self, query: &str, limit: u8) -> ScResult<Vec<UserIdentity>> {
        self.seen_user_searches
            .lock()
            .unwrap()
            .push((query.to_string(), limit));
        Ok(vec![
            UserIdentity {
                login: "octocat".into(),
                id: Some(583_231),
                name: Some("The Octocat".into()),
                avatar_url: Some("https://avatars.example/u/1".into()),
                html_url: Some("https://github.com/octocat".into()),
            },
            UserIdentity {
                login: "octodog".into(),
                id: Some(7),
                name: None,
                avatar_url: None,
                html_url: None,
            },
        ])
    }
    async fn list_repos(&self, page: PageParams) -> ScResult<Page<Repo>> {
        if self.paginate {
            // A two-page sequence driven by the opaque cursor that the
            // services layer round-trips through `nextToken`.
            let p = page
                .cursor
                .as_deref()
                .and_then(|c| c.parse::<u64>().ok())
                .unwrap_or(1);
            let repo = Repo {
                owner_avatar_url: None,
                owner: "octocat".into(),
                name: format!("repo{p}"),
                url: Some(format!("https://github.com/octocat/repo{p}")),
                default_branch: Some("main".into()),
                created_at: None,
                updated_at: None,
            };
            let next_cursor = if p < 2 {
                Some((p + 1).to_string())
            } else {
                None
            };
            return Ok(Page {
                items: vec![repo],
                next_cursor,
            });
        }
        Ok(Page {
            items: vec![Repo {
                owner_avatar_url: None,
                owner: "octocat".into(),
                name: "hello".into(),
                url: Some("https://github.com/octocat/hello".into()),
                default_branch: Some("main".into()),
                created_at: None,
                updated_at: None,
            }],
            next_cursor: None,
        })
    }
    async fn search_repos(&self, _: &str, page: PageParams) -> ScResult<Page<Repo>> {
        self.list_repos(page).await
    }
    async fn get_repo(&self, owner: &str, name: &str) -> ScResult<Repo> {
        if owner == "ghost" {
            return Err(ScError::NotFound("no such repo".into()));
        }
        Ok(Repo {
            owner_avatar_url: None,
            owner: owner.into(),
            name: name.into(),
            url: Some(format!("https://github.com/{owner}/{name}")),
            default_branch: Some("main".into()),
            created_at: None,
            updated_at: None,
        })
    }
    async fn list_remote_branches(
        &self,
        _: &str,
        _: &str,
        prefix: Option<&str>,
        _: PageParams,
    ) -> ScResult<Page<Branch>> {
        let items = ["main", "dev"]
            .iter()
            .filter(|n| prefix.is_none_or(|p| n.starts_with(p)))
            .map(|n| Branch {
                name: (*n).into(),
                commit_sha: None,
                protected: false,
            })
            .collect();
        Ok(Page {
            items,
            next_cursor: None,
        })
    }
    async fn get_file_content(
        &self,
        _: &RepoRef,
        _: &str,
        _: Option<&str>,
    ) -> ScResult<Option<String>> {
        if self.file_content_decode_error {
            return Err(intent_sourcecontrol::Error::Decode(
                "contents path is a directory, not a file".into(),
            ));
        }
        Ok(self.file_content.clone())
    }
    async fn create_pr(&self, _: &RepoRef, input: NewPullRequest) -> ScResult<PullRequest> {
        self.create_pr_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if let Some(error) = self.create_pr_error {
            return Err(error());
        }
        Ok(PullRequest {
            number: 7,
            url: "https://github.com/o/r/pull/7".into(),
            title: input.title,
            body: input.body,
            state: PrState::Open,
            draft: input.draft,
            source_branch: input.source_branch,
            target_branch: input.target_branch,
            author: "octocat".into(),
            mergeable: Some(true),
            mergeable_state: Some("clean".into()),
            head_sha: Some("deadbeef".into()),
            created_at: String::new(),
            updated_at: String::new(),
        })
    }
    async fn rate_limit_status(&self) -> ScResult<RateLimitStatus> {
        *self.seen_reset_probes.lock().unwrap() += 1;
        Ok(RateLimitStatus {
            reset_at: self.rate_limit_reset,
            remaining: self.rate_limit_remaining,
            limit: self.rate_limit_limit,
        })
    }
    async fn get_pr(&self, _: &RepoRef, number: u64) -> ScResult<PullRequest> {
        self.seen_get_pr.lock().unwrap().push(number);
        if let Some(error) = self.get_pr_error {
            return Err(error());
        }
        if self.rate_limited {
            return Err(ScError::RateLimited(
                "API rate limit exceeded for user ID 526899.".into(),
            ));
        }
        if self.hang_get_pr == Some(number) {
            // A TCP connection that went dark: the future never resolves.
            std::future::pending::<()>().await;
        }
        if let Some(park) = &self.get_pr_park {
            if !park.claimed.swap(true, std::sync::atomic::Ordering::SeqCst) {
                park.entered.notify_one();
                park.release.notified().await;
            }
        }
        if self.missing_pr == Some(number) {
            return Err(ScError::NotFound("no such PR".into()));
        }
        let mut pr = sample_pr();
        pr.number = number;
        pr.url = format!("https://github.com/o/r/pull/{number}");
        if self.merged_linked {
            pr.state = PrState::Merged;
        }
        if self.dirty_pr {
            pr.mergeable = Some(false);
            pr.mergeable_state = Some("dirty".into());
        }
        if self.blocked_pr {
            pr.mergeable = Some(false);
            pr.mergeable_state = Some("blocked".into());
        }
        Ok(pr)
    }
    async fn list_prs(&self, repo: &RepoRef, query: PrQuery) -> ScResult<Page<PullRequest>> {
        self.seen_pr_queries.lock().unwrap().push(query.clone());
        if self.rate_limited || self.rate_limited_list_prs {
            return Err(ScError::RateLimited(
                "API rate limit exceeded for user ID 526899.".into(),
            ));
        }
        if self.fail_list_prs {
            return Err(intent_sourcecontrol::Error::Api("list_prs down".into()));
        }
        if !query.extra_repos.is_empty() {
            let items = multi_repo_hits(repo, &query.extra_repos)
                .into_iter()
                .map(|(number, repo_url, updated_at)| PullRequest {
                    number,
                    url: format!("{repo_url}/pull/{number}"),
                    updated_at,
                    ..sample_pr()
                })
                .collect();
            return Ok(Page {
                items,
                next_cursor: Some("2".into()),
            });
        }
        let mut items = if self.discover {
            vec![sample_pr()]
        } else {
            vec![]
        };
        if let Some(n) = self.open_pr_number {
            let mut pr = sample_pr();
            pr.number = n;
            pr.url = format!("https://github.com/o/r/pull/{n}");
            items.push(pr);
        }
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
        method: MergeMethod,
        _: MergeOptions,
    ) -> ScResult<MergeOutcome> {
        Ok(MergeOutcome {
            merged: true,
            message: format!("Merged via {method:?}"),
            sha: Some("mergedsha".into()),
        })
    }
    async fn mergeability(&self, _: &RepoRef, _: u64) -> ScResult<Mergeability> {
        unimplemented!()
    }
    async fn update_branch(&self, _: &RepoRef, _: u64) -> ScResult<()> {
        Ok(())
    }
    async fn submit_review(
        &self,
        _: &RepoRef,
        _: u64,
        verdict: ReviewVerdict,
        body: Option<String>,
    ) -> ScResult<Review> {
        Ok(Review {
            author: "octocat".into(),
            verdict,
            body,
            submitted_at: "2026-06-17T05:00:00.000Z".into(),
            id: None,
            commit_sha: None,
        })
    }
    async fn review_decision(&self, _: &RepoRef, _: u64) -> ScResult<Option<ReviewDecision>> {
        if self.fail_review_decision {
            return Err(ScError::Api("graphql down".into()));
        }
        Ok(self.review_decision)
    }
    async fn list_reviews(&self, _: &RepoRef, _: u64) -> ScResult<Vec<Review>> {
        if self.no_reviews {
            return Ok(vec![]);
        }
        Ok(vec![
            Review {
                author: "alice".into(),
                verdict: ReviewVerdict::Approve,
                body: None,
                submitted_at: "2026-01-01".into(),
                id: None,
                commit_sha: None,
            },
            Review {
                author: "bob".into(),
                verdict: ReviewVerdict::Comment,
                body: None,
                submitted_at: "2026-01-02".into(),
                id: None,
                commit_sha: None,
            },
        ])
    }
    async fn merge_requirements(
        &self,
        _: &RepoRef,
        _: u64,
    ) -> ScResult<MergeRequirementSignals> {
        match &self.merge_signals {
            Some(signals) => Ok(signals.clone()),
            None => Err(ScError::Unsupported("merge requirements probe".into())),
        }
    }
    async fn list_comments(&self, _: &RepoRef, _: u64) -> ScResult<Vec<Comment>> {
        if let Some(hook) = self.on_list_comments.lock().unwrap().as_ref() {
            hook();
        }
        Ok(vec![Comment {
            id: "1".into(),
            author: "u".into(),
            body: "hi".into(),
            path: None,
            line: None,
            created_at: "2026".into(),
            url: None,
        }])
    }
    async fn add_comment(
        &self,
        _: &RepoRef,
        _: u64,
        body: &str,
        _: Option<CommentAnchor>,
    ) -> ScResult<Comment> {
        Ok(Comment {
            id: "777".into(),
            author: "octocat".into(),
            body: body.to_string(),
            path: None,
            line: None,
            created_at: "2026".into(),
            url: Some("https://github.com/o/r/pull/42#issuecomment-777".into()),
        })
    }
    async fn list_review_comments(
        &self,
        _: &RepoRef,
        _: u64,
        page: PageParams,
    ) -> ScResult<Page<ReviewComment>> {
        if self.fail_review_comments {
            return Err(ScError::Api("rest down".into()));
        }
        if self.review_comment_pages > 0 {
            let n = page
                .cursor
                .as_deref()
                .and_then(|c| c.parse::<u64>().ok())
                .unwrap_or(1);
            let next_cursor = (n < self.review_comment_pages).then(|| (n + 1).to_string());
            return Ok(Page {
                items: vec![ReviewComment {
                    id: n,
                    body: format!("nit {n}"),
                    path: "a.rs".into(),
                    line: Some(1),
                    author: "rev".into(),
                    created_at: "2026".into(),
                    updated_at: "2026".into(),
                    in_reply_to_id: None,
                    url: "url".into(),
                }],
                next_cursor,
            });
        }
        Ok(Page {
            items: vec![ReviewComment {
                id: 5,
                body: "nit".into(),
                path: "a.rs".into(),
                line: Some(1),
                author: "rev".into(),
                created_at: "2026".into(),
                updated_at: "2026".into(),
                in_reply_to_id: None,
                url: "url".into(),
            }],
            next_cursor: None,
        })
    }
    async fn reply_to_review_comment(
        &self,
        _: &RepoRef,
        _: u64,
        comment_id: u64,
        body: &str,
    ) -> ScResult<ReviewComment> {
        Ok(ReviewComment {
            id: comment_id + 1,
            body: body.to_string(),
            path: "a.rs".into(),
            line: Some(1),
            author: "octocat".into(),
            created_at: "2026".into(),
            updated_at: "2026".into(),
            in_reply_to_id: Some(comment_id),
            url: "https://github.com/o/r/pull/42#discussion_r999".into(),
        })
    }
    async fn get_review_threads(
        &self,
        _: &RepoRef,
        _: u64,
        page: PageParams,
    ) -> ScResult<Page<ReviewThread>> {
        if self.rate_limited_threads {
            return Err(ScError::RateLimited(
                "API rate limit exceeded for user ID 526899.".into(),
            ));
        }
        if self.fail_threads {
            return Err(ScError::Api("graphql down".into()));
        }
        if self.thread_pages > 0 {
            let n = page
                .cursor
                .as_deref()
                .and_then(|c| c.parse::<u64>().ok())
                .unwrap_or(1);
            let next_cursor = (n < self.thread_pages).then(|| (n + 1).to_string());
            return Ok(Page {
                items: vec![ReviewThread {
                    id: format!("RT{n}"),
                    is_resolved: false,
                    comments: vec![ReviewThreadComment {
                        id: format!("c{n}"),
                        body: "x".into(),
                        author: "rev".into(),
                        path: "a.rs".into(),
                        line: Some(1),
                        created_at: "2026".into(),
                    }],
                }],
                next_cursor,
            });
        }
        Ok(Page {
            items: vec![
                ReviewThread {
                    id: "RT1".into(),
                    is_resolved: false,
                    comments: vec![ReviewThreadComment {
                        id: "c1".into(),
                        body: "x".into(),
                        author: "rev".into(),
                        path: "a.rs".into(),
                        line: Some(1),
                        created_at: "2026".into(),
                    }],
                },
                ReviewThread {
                    id: "RT2".into(),
                    is_resolved: true,
                    comments: vec![ReviewThreadComment {
                        id: "c2".into(),
                        body: "y".into(),
                        author: "rev".into(),
                        path: "b.rs".into(),
                        line: Some(2),
                        created_at: "2026".into(),
                    }],
                },
            ],
            next_cursor: None,
        })
    }
    async fn resolve_thread(&self, _: &str) -> ScResult<bool> {
        Ok(true)
    }
    async fn unresolve_thread(&self, _: &str) -> ScResult<bool> {
        Ok(false)
    }
    async fn check_runs(&self, _: &RepoRef, _: &str) -> ScResult<Vec<CheckRun>> {
        Ok(vec![
            CheckRun {
                name: "build".into(),
                state: CheckState::Success,
                url: None,
                started_at: None,
            },
            CheckRun {
                name: "test".into(),
                state: CheckState::Failure,
                url: None,
                started_at: None,
            },
            CheckRun {
                name: "lint".into(),
                state: CheckState::Pending,
                url: None,
                started_at: None,
            },
        ])
    }
    async fn create_issue(&self, _: &RepoRef, _: &str, _: Option<&str>) -> ScResult<Issue> {
        unimplemented!()
    }
    async fn get_issue(&self, _: &RepoRef, number: u64) -> ScResult<Issue> {
        self.seen_get_issue.lock().unwrap().push(number);
        if self.rate_limited {
            return Err(ScError::RateLimited(
                "API rate limit exceeded for user ID 526899.".into(),
            ));
        }
        if self.missing_issue == Some(number) {
            return Err(ScError::NotFound("no such issue".into()));
        }
        Ok(Issue {
            number,
            ..stub_issue()
        })
    }
    async fn list_issues(&self, repo: &RepoRef, query: IssueQuery) -> ScResult<Page<Issue>> {
        self.seen_issue_queries.lock().unwrap().push(query.clone());
        if !query.extra_repos.is_empty() {
            let items = multi_repo_hits(repo, &query.extra_repos)
                .into_iter()
                .map(|(number, repo_url, updated_at)| Issue {
                    number,
                    url: format!("{repo_url}/issues/{number}"),
                    updated_at,
                    ..stub_issue()
                })
                .collect();
            return Ok(Page {
                items,
                next_cursor: Some("2".into()),
            });
        }
        Ok(Page {
            items: vec![stub_issue()],
            next_cursor: None,
        })
    }
}

pub fn stub_issue() -> Issue {
    Issue {
        number: 11,
        title: "Bug report".into(),
        body: Some("something broke".into()),
        state: "open".into(),
        url: "https://github.com/o/r/issues/11".into(),
        author: "reporter".into(),
        created_at: "2026-01-01T00:00:00Z".into(),
        updated_at: "2026-01-02T00:00:00Z".into(),
    }
}
