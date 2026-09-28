//! Projection contracts. Loopback provider cases use explicit injected credentials,
//! not real repository authority, credential ownership or runtime registration.

use super::*;
use intent_core::RepositoryTarget;
use intent_sourcecontrol::{
    BranchRules, CheckState, MergeQueueRemoval, MergeRequirementSignals, PrState, PullRequest,
    Review, ReviewAvailability, ReviewThreadTally, ReviewVerdict, RollupCheck, RollupCheckKind,
};

fn target() -> ReviewTarget {
    ReviewTarget {
        repository: RepositoryTarget {
            provider: RepositoryProvider::Gitlab,
            instance_base_url: "https://forge.example:8443/install".into(),
            project_path: "Team/SubGroup/Project".into(),
        },
        kind: RepositoryResourceKind::MergeRequest,
        number: 17,
    }
}

fn observation() -> ReviewObservation {
    ReviewObservation {
        details: ReviewDetails {
            review: PullRequest {
                number: 17,
                url: "https://forge.example:8443/install/Team/SubGroup/Project/-/merge_requests/17"
                    .into(),
                title: "Actual title".into(),
                body: Some("Actual body".into()),
                state: PrState::Open,
                draft: false,
                source_branch: "topic".into(),
                target_branch: "main".into(),
                author: "alice".into(),
                mergeable: Some(true),
                mergeable_state: Some("clean".into()),
                head_sha: Some("actual-head".into()),
                created_at: "2026-09-27T00:00:00Z".into(),
                updated_at: "2026-09-28T00:00:00Z".into(),
            },
            source: Some(ReviewBranchIdentity {
                instance_base_url: target().repository.instance_base_url,
                project_id: 9_007_199_254_740_993,
                project_path: None,
                branch: "topic".into(),
            }),
            target: None,
            confirmed_draft: Some(false),
            confirmed_state: Some(ConfirmedReviewState::Open),
        },
        signals: MergeRequirementSignals {
            merge_state_status: Some("mergeable".into()),
            review_decision: Some(ReviewDecision::Approved),
            checks: vec![check("build", CheckState::Success, true)],
            checks_known: true,
            checks_head_sha: Some("actual-head".into()),
            branch_rules: Some(BranchRules {
                required_approving_review_count: Some(1),
                required_conversation_resolution: Some(true),
                required_status_checks: vec!["build".into()],
            }),
            ..Default::default()
        },
        reviews: Some(vec![review("alice", ReviewVerdict::Approve, "2026-09-28")]),
        threads: Some(ReviewThreadTally {
            review_comment_count: 2,
            unresolved: 0,
        }),
        conversation_count: Some(3),
        availability: ReviewAvailability {
            policy: ProviderAvailability::Available,
            approvals: ProviderAvailability::Available,
            checks: ProviderAvailability::Available,
            discussions: ProviderAvailability::Available,
        },
    }
}

fn check(name: &str, state: CheckState, required: bool) -> RollupCheck {
    RollupCheck {
        name: name.into(),
        kind: RollupCheckKind::CheckRun,
        state,
        is_required: required,
        url: None,
        started_at: None,
    }
}
fn review(author: &str, verdict: ReviewVerdict, submitted_at: &str) -> Review {
    Review {
        author: author.into(),
        verdict,
        body: None,
        submitted_at: submitted_at.into(),
    }
}
fn project(observation: &ReviewObservation) -> Value {
    super::super::qualified_review_snapshot(&target(), observation).unwrap()
}

#[test]
fn complete_flat_snapshot_and_native_detail_have_exact_contract() {
    let value = project(&observation());
    let details = json!({"resource":target(),"url":observation().details.review.url,
        "title":"Actual title","body":"Actual body","state":"open","draft":false,
        "sourceBranch":"topic","targetBranch":"main","source":{
            "provider":"gitlab","instanceBaseUrl":"https://forge.example:8443/install",
            "projectId":"9007199254740993","projectPath":null,"branch":"topic"},
        "target":null,"author":"alice","mergeable":true,"mergeableState":"clean",
        "headSha":"actual-head","createdAt":"2026-09-27T00:00:00Z","updatedAt":"2026-09-28T00:00:00Z"});
    assert_eq!(
        value,
        json!({
            "repo":"Team/SubGroup/Project","prNumber":17,"title":"Actual title","url":observation().details.review.url,
            "state":"open","isDraft":false,"isMerged":false,"isClosed":false,"headSha":"actual-head",
            "updatedAt":"2026-09-28T00:00:00Z","mergeable":true,"mergeableState":"clean","mergeBlockedReason":null,
            "checks":{"total":1,"passed":1,"failed":0,"pending":0,"failedNames":[]},
            "reviews":{"decision":"approved","approvals":1,"changesRequested":0},
            "comments":{"conversationCount":3,"reviewCommentCount":2,"totalCount":5,"unresolvedThreadCount":0},
            "requirements":{"state":"open","isDraft":false,"hasConflicts":false,"isBehind":false,
                "mergeable":true,"mergeStateStatus":"mergeable","rulesKnown":true,
                "checks":{"total":1,"passed":1,"failed":0,"pending":0,"items":[{"name":"build","status":"passed","required":true}],
                    "failingRequired":[],"pendingRequired":[],"requiredKnown":true},
                "approvals":{"decision":"approved","have":1,"changesRequested":0,"needed":1},
                "threads":{"unresolved":0,"resolutionRequired":true}},
            "resource":target(),"details":details,
            "availability":{"policy":"available","approvals":"available","checks":"available","discussions":"available"}
        })
    );
    let _: NativeReviewDetails = serde_json::from_value(value["details"].clone()).unwrap();
}

#[test]
fn incompatible_provider_kind_and_number_return_typed_refusal() {
    for i in 0..6 {
        let mut t = target();
        let mut o = observation();
        match i {
            0 => t.repository.provider = RepositoryProvider::Github,
            1 => t.kind = RepositoryResourceKind::PullRequest,
            2 => t.kind = RepositoryResourceKind::Issue,
            3 => t.number = 0,
            4 => t.number = 18,
            _ => o.details.review.number = 0,
        }
        assert!(matches!(
            qualified_review_snapshot(&t, &o),
            Err(Error::InvalidParams(_))
        ));
    }
}

#[test]
fn malformed_required_metadata_and_contradictory_branch_identity_refuse() {
    for i in 0..8 {
        let mut o = observation();
        match i {
            0 => o.details.review.url.clear(),
            1 => o.details.review.title.clear(),
            2 => o.details.review.created_at.clear(),
            3 => o.details.review.updated_at.clear(),
            4 => o.details.source.as_mut().unwrap().project_id = 0,
            5 => {
                o.details.source.as_mut().unwrap().instance_base_url =
                    "https://other.example".into();
            }
            6 => o.details.source.as_mut().unwrap().branch.clear(),
            _ => {
                o.details.target = o.details.source.clone();
                o.details.target.as_mut().unwrap().project_path = Some("wrong/project".into());
            }
        }
        assert!(matches!(
            qualified_review_snapshot(&target(), &o),
            Err(Error::Internal(_))
        ));
    }
}

#[test]
fn confirmed_lifecycle_and_draft_never_borrow_legacy_defaults() {
    for (confirmed, word) in [
        (Some(ConfirmedReviewState::Open), "open"),
        (Some(ConfirmedReviewState::Locked), "locked"),
        (Some(ConfirmedReviewState::Closed), "closed"),
        (Some(ConfirmedReviewState::Merged), "merged"),
        (None, "unknown"),
    ] {
        for draft in [None, Some(false), Some(true)] {
            let mut o = observation();
            o.details.confirmed_state = confirmed;
            o.details.confirmed_draft = draft;
            o.details.review.state = PrState::Merged;
            o.details.review.draft = true;
            let v = project(&o);
            assert_eq!(
                v["state"],
                if confirmed == Some(ConfirmedReviewState::Open) && draft == Some(true) {
                    "draft"
                } else {
                    word
                }
            );
            assert_eq!(
                v["details"]["state"],
                confirmed.map(|_| word).map_or(Value::Null, |s| json!(s))
            );
            assert_eq!(v["isDraft"], json!(draft));
            assert_eq!(v["requirements"]["isDraft"], json!(draft));
            assert_eq!(
                v["isMerged"],
                json!(confirmed.map(|s| s == ConfirmedReviewState::Merged))
            );
            assert_eq!(
                v["isClosed"],
                json!(confirmed.map(|s| s == ConfirmedReviewState::Closed))
            );
        }
    }
}

#[test]
fn absent_branch_and_optional_detail_fields_remain_null() {
    let mut o = observation();
    o.details.source = None;
    o.details.target = None;
    o.details.review.source_branch.clear();
    o.details.review.target_branch.clear();
    o.details.review.author.clear();
    o.details.review.body = None;
    o.details.review.head_sha = None;
    o.details.review.mergeable = None;
    o.details.review.mergeable_state = None;
    let v = project(&o);
    for key in [
        "source",
        "target",
        "sourceBranch",
        "targetBranch",
        "author",
        "body",
        "headSha",
        "mergeable",
        "mergeableState",
    ] {
        assert_eq!(v["details"][key], Value::Null, "{key}");
    }
}

#[test]
fn project_ids_preserve_every_u64_bit_and_source_paths_are_not_invented() {
    for id in [9_007_199_254_740_993, u64::MAX] {
        let mut o = observation();
        o.details.source.as_mut().unwrap().project_id = id;
        let v = project(&o);
        assert_eq!(v["details"]["source"]["projectId"], id.to_string());
        assert_eq!(v["details"]["source"]["projectPath"], Value::Null);
        assert_eq!(v["resource"], json!(target()));
    }
}

#[test]
fn normalized_author_preserves_literal_ghost_and_empty_stays_null() {
    for (author, expected) in [
        ("ghost", json!("ghost")),
        ("alice", json!("alice")),
        ("", Value::Null),
    ] {
        let mut o = observation();
        o.details.review.author = author.into();
        let value = project(&o);
        assert_eq!(value["details"]["author"], expected);
        assert!(value["details"].get("authorConfirmed").is_none());
    }
}

#[test]
fn unknown_checks_differ_from_a_known_empty_rollup() {
    let mut o = observation();
    o.signals.checks.clear();
    let empty = project(&o);
    assert_eq!(empty["checks"]["total"], 0);
    assert_eq!(empty["requirements"]["checks"]["items"], json!([]));
    for state in [
        ProviderAvailability::Restricted,
        ProviderAvailability::Unavailable,
        ProviderAvailability::Transient,
        ProviderAvailability::RateLimited,
        ProviderAvailability::Unknown,
    ] {
        o.availability.checks = state;
        let v = project(&o);
        for key in ["total", "passed", "failed", "pending", "failedNames"] {
            assert_eq!(v["checks"][key], Value::Null);
        }
        for key in ["items", "failingRequired", "pendingRequired"] {
            assert_eq!(v["requirements"]["checks"][key], Value::Null);
        }
        assert_eq!(v["requirements"]["checks"]["requiredKnown"], false);
        assert_eq!(v["availability"]["checks"], json!(state));
    }
    o.availability.checks = ProviderAvailability::Available;
    o.signals.checks_known = false;
    assert_eq!(project(&o)["checks"]["total"], Value::Null);
}

#[test]
fn different_or_missing_actual_head_invalidates_reported_check_head_only() {
    for head in [None, Some("other-head".into())] {
        let mut o = observation();
        o.details.review.head_sha = head;
        let v = project(&o);
        assert_eq!(v["checks"]["total"], Value::Null);
        assert_eq!(v["availability"]["checks"], "unknown");
        assert_eq!(v["title"], "Actual title");
        assert_eq!(v["reviews"]["approvals"], 1);
    }
    let mut o = observation();
    o.signals.checks_head_sha = None;
    assert_eq!(project(&o)["checks"]["passed"], 1);
}

#[test]
fn latest_actionable_reviews_do_not_synthesize_a_provider_decision() {
    let mut o = observation();
    o.signals.review_decision = None;
    o.reviews = Some(vec![
        review("alice", ReviewVerdict::Approve, "1"),
        review("alice", ReviewVerdict::RequestChanges, "2"),
        review("alice", ReviewVerdict::Comment, "3"),
        review("bob", ReviewVerdict::Approve, "1"),
    ]);
    let v = project(&o);
    assert_eq!(
        v["reviews"],
        json!({"decision":null,"approvals":1,"changesRequested":1})
    );
    for (d, s) in [
        (ReviewDecision::Approved, "approved"),
        (ReviewDecision::ChangesRequested, "changes_requested"),
        (ReviewDecision::ReviewRequired, "review_required"),
    ] {
        o.signals.review_decision = Some(d);
        o.reviews = None;
        let v = project(&o);
        assert_eq!(
            v["reviews"],
            json!({"decision":s,"approvals":null,"changesRequested":null})
        );
    }
}

#[test]
fn restricted_approvals_discussions_and_unknown_policy_remain_partial() {
    let mut o = observation();
    o.reviews = None;
    o.threads = None;
    o.conversation_count = None;
    o.signals.review_decision = None;
    o.availability.approvals = ProviderAvailability::Restricted;
    o.availability.discussions = ProviderAvailability::Unavailable;
    o.availability.policy = ProviderAvailability::Unknown;
    let v = project(&o);
    assert_eq!(
        v["reviews"],
        json!({"decision":null,"approvals":null,"changesRequested":null})
    );
    assert_eq!(
        v["comments"],
        json!({"conversationCount":null,"reviewCommentCount":null,"totalCount":null})
    );
    assert_eq!(v["requirements"]["threads"], json!({}));
    assert!(v["requirements"]["approvals"].get("needed").is_none());
    assert_eq!(v["requirements"]["rulesKnown"], false);
    assert_eq!(v["checks"]["passed"], 1);
    assert_eq!(v["availability"]["approvals"], "restricted");
    assert_eq!(v["availability"]["discussions"], "unavailable");
}

#[test]
fn individual_comment_evidence_is_preserved_without_a_fabricated_total() {
    let mut o = observation();
    o.conversation_count = None;
    let v = project(&o);
    assert_eq!(v["comments"]["reviewCommentCount"], 2);
    assert_eq!(v["comments"]["totalCount"], Value::Null);
    assert_eq!(v["comments"]["unresolvedThreadCount"], 0);
    o.conversation_count = Some(0);
    o.threads = None;
    let v = project(&o);
    assert_eq!(v["comments"]["conversationCount"], 0);
    assert_eq!(v["comments"]["totalCount"], Value::Null);
    assert!(v["requirements"]["threads"].get("unresolved").is_none());
}

#[test]
fn invalid_counts_and_total_overflow_are_typed_errors() {
    for (a, b) in [(-1, 0), (0, -1), (i64::MAX, 1)] {
        let mut o = observation();
        o.conversation_count = Some(a);
        o.threads.as_mut().unwrap().review_comment_count = b;
        assert!(matches!(
            qualified_review_snapshot(&target(), &o),
            Err(Error::Internal(_))
        ));
    }
}

#[test]
fn unknown_merge_facts_do_not_claim_false_or_fabricate_a_blocked_reason() {
    let mut o = observation();
    o.details.review.mergeable = None;
    o.details.review.mergeable_state = Some("unknown".into());
    o.signals.merge_state_status = None;
    let v = project(&o);
    assert_eq!(v["requirements"]["hasConflicts"], Value::Null);
    assert_eq!(v["requirements"]["isBehind"], Value::Null);
    assert!(v["requirements"].get("mergeable").is_none());
    assert_eq!(v["mergeBlockedReason"], Value::Null);
    o.details.review.mergeable_state = Some("blocked".into());
    o.availability.policy = ProviderAvailability::Unknown;
    assert_eq!(project(&o)["mergeBlockedReason"], Value::Null);
    o.details.review.mergeable = Some(false);
    assert_eq!(project(&o)["mergeBlockedReason"], "not mergeable");
}

#[test]
fn mergeability_alone_does_not_establish_branch_or_conflict_status() {
    let mut o = observation();
    o.details.review.mergeable_state = None;
    o.signals.merge_state_status = None;
    let v = project(&o);
    assert_eq!(v["mergeable"], true);
    assert_eq!(v["requirements"]["isBehind"], Value::Null);
    assert_eq!(v["requirements"]["hasConflicts"], Value::Null);
}

#[test]
fn observed_conflicts_behind_and_draft_use_the_existing_reason_words() {
    for (raw, key, reason) in [
        ("conflict", "hasConflicts", "merge conflicts"),
        ("need_rebase", "isBehind", "branch behind base"),
    ] {
        let mut o = observation();
        o.details.review.mergeable = None;
        o.details.review.mergeable_state = None;
        o.signals.merge_state_status = Some(raw.into());
        let v = project(&o);
        assert_eq!(v["requirements"][key], true);
        assert_eq!(v["mergeBlockedReason"], reason);
    }
    let mut o = observation();
    o.details.confirmed_draft = Some(true);
    assert_eq!(
        project(&o)["mergeBlockedReason"],
        "draft PRs cannot be merged"
    );
}

#[test]
fn queue_ejection_fields_and_deadline_are_only_present_when_observed() {
    let mut o = observation();
    let v = project(&o);
    for key in ["isInMergeQueue", "mergeQueueEjection"] {
        assert!(v["requirements"].get(key).is_none());
    }
    assert!(v.get("pausedUntil").is_none());
    o.signals.is_in_merge_queue = Some(true);
    o.signals.merge_queue_removal = Some(MergeQueueRemoval {
        at: "2026-09-28T00:00:00Z".into(),
        reason: None,
    });
    let v = project(&o);
    assert_eq!(v["requirements"]["isInMergeQueue"], true);
    assert_eq!(
        v["requirements"]["mergeQueueEjection"],
        json!({"at":"2026-09-28T00:00:00Z"})
    );
}

#[test]
fn live_check_attempts_and_required_failures_share_the_legacy_pure_fold() {
    let mut o = observation();
    o.signals.checks = vec![
        check("build", CheckState::Failure, true),
        check("build", CheckState::Pending, true),
        check("lint", CheckState::Failure, false),
    ];
    let v = project(&o);
    assert_eq!(
        v["checks"],
        json!({"total":2,"passed":0,"failed":1,"pending":1,"failedNames":["lint"]})
    );
    assert_eq!(v["requirements"]["checks"]["failingRequired"], json!([]));
    assert_eq!(
        v["requirements"]["checks"]["pendingRequired"],
        json!(["build"])
    );
}

#[test]
fn pure_projection_is_repeatable_without_mutating_the_observation() {
    let o = observation();
    let before = o.clone();
    let first = project(&o);
    assert_eq!(first, project(&o));
    assert_eq!(o, before);
}

struct FixtureCredentials;
#[async_trait::async_trait]
impl intent_sourcecontrol::GitlabRequestCredentials for FixtureCredentials {
    async fn token_for(
        &self,
        _: &intent_sourcecontrol::GitlabInstance,
    ) -> intent_sourcecontrol::Result<intent_sourcecontrol::SecretString> {
        Ok("loopback-fixture-token".to_string().into())
    }
}

struct ProviderFixture {
    provider: intent_sourcecontrol::GitLabSourceControl,
    requests: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for ProviderFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl ProviderFixture {
    async fn new(approvals_status: u16, malformed_primary: bool) -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = requests.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                while !bytes.ends_with(b"\r\n\r\n") {
                    let b = socket.read_u8().await.unwrap();
                    bytes.push(b);
                    assert!(bytes.len() < 16_384);
                }
                let request = String::from_utf8(bytes).unwrap();
                let path = request.split_whitespace().nth(1).unwrap().to_owned();
                seen.lock().unwrap().push(path.clone());
                let (status, body) = if path.contains("/approvals") {
                    (
                        approvals_status,
                        json!({"approvals_required":1,"approvals_left":0,"approved_by":[]}),
                    )
                } else if path.contains("/discussions") {
                    (200, json!([]))
                } else if path.ends_with("/merge_requests/17") {
                    let mut body = json!({"iid":17,"web_url":observation().details.review.url,"title":"Provider title",
                            "state":"opened","draft":false,"source_branch":"topic","target_branch":"main",
                            "source_project_id":9_007_199_254_740_993_u64,"target_project_id":8,"sha":"actual-head",
                            "created_at":"2026-09-27T00:00:00Z","updated_at":"2026-09-28T00:00:00Z",
                            "detailed_merge_status":"mergeable","author":{"username":"alice"}});
                    if malformed_primary {
                        body.as_object_mut().unwrap().remove("iid");
                    }
                    (200, body)
                } else {
                    (
                        200,
                        json!({"id":8,"only_allow_merge_if_pipeline_succeeds":false,"only_allow_merge_if_all_discussions_are_resolved":true}),
                    )
                };
                let body = body.to_string();
                let quota = if status == 429 {
                    "ratelimit-remaining: 0\r\nratelimit-reset: 1800000000\r\n"
                } else {
                    ""
                };
                let response=format!("HTTP/1.1 {status} Fixture\r\ncontent-type: application/json\r\ncontent-length: {}\r\n{quota}connection: close\r\n\r\n{body}",body.len());
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let instance =
            intent_sourcecontrol::GitlabInstance::parse(&target().repository.instance_base_url)
                .unwrap();
        let descriptor =
            intent_sourcecontrol::GitlabDescriptor::with_loopback_endpoint(instance, &endpoint)
                .unwrap();
        let provider = intent_sourcecontrol::GitLabSourceControl::new(
            descriptor,
            std::sync::Arc::new(FixtureCredentials),
        )
        .unwrap();
        Self {
            provider,
            requests,
            task,
        }
    }
}

#[tokio::test]
async fn actual_optional_429_keeps_primary_and_quota_without_hidden_projection_reads() {
    use intent_sourcecontrol::SourceControl;
    let f = ProviderFixture::new(429, false).await;
    let o = f
        .provider
        .observe_review(
            &intent_sourcecontrol::RepoRef::new("Team/SubGroup", "Project"),
            17,
        )
        .await
        .unwrap();
    assert_eq!(o.availability.approvals, ProviderAvailability::RateLimited);
    let quota = f.provider.rate_limit_status().await.unwrap();
    assert_eq!(quota.remaining, Some(0));
    assert_eq!(quota.reset_at, Some(1_800_000_000));
    let before = f.requests.lock().unwrap().clone();
    assert_eq!(before.len(), 4);
    let v = project(&o);
    assert_eq!(v["title"], "Provider title");
    assert_eq!(v["availability"]["approvals"], "rate-limited");
    assert_eq!(v["reviews"]["approvals"], Value::Null);
    assert_eq!(v["checks"]["total"], 0);
    assert_eq!(v["comments"]["totalCount"], 0);
    assert!(v.get("pausedUntil").is_none());
    assert_eq!(*f.requests.lock().unwrap(), before);
    assert_eq!(f.provider.rate_limit_status().await.unwrap(), quota);
}

#[tokio::test]
async fn malformed_primary_remains_provider_decode_error_before_projection() {
    let f = ProviderFixture::new(200, true).await;
    let result = f
        .provider
        .observe_review(
            &intent_sourcecontrol::RepoRef::new("Team/SubGroup", "Project"),
            17,
        )
        .await;
    assert!(matches!(
        result,
        Err(intent_sourcecontrol::Error::Decode(_))
    ));
    assert_eq!(f.requests.lock().unwrap().len(), 1);
}
