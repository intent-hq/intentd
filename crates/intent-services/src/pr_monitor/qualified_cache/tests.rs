use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use intent_core::{
    ExecutionScope, RepositoryConnectionScope, RepositoryProvider, RepositoryTarget,
};
use intent_sourcecontrol::{
    error::{ProviderFailure, ProviderFailureKind},
    ConfirmedReviewState, Issue, MergeRequirementSignals, PrState, ProviderAvailability,
    PullRequest, ReviewAvailability, ReviewBranchIdentity, ReviewThreadTally,
};
use tokio::sync::oneshot;

use super::*;
use crate::issue_cache::{read_qualified_issue, IssueCache, IssueCacheEntry};
use crate::pr_monitor::{NONE, PR_CACHE_MAX_ENTRIES, PR_CACHE_MAX_IDLE};

fn connection(account: &str) -> ConnectionObservations {
    ConnectionObservations::new(
        ExecutionScope {
            daemon_id: "daemon".into(),
            authority_scope_id: "owner".into(),
            authority_generation: 9_007_199_254_740_995,
        },
        RepositoryConnectionScope {
            connection_id: "gitlab".into(),
            account_id: account.into(),
            connection_generation: u64::MAX,
        },
    )
}

fn target(project: &str, kind: RepositoryResourceKind, number: u64) -> ReviewTarget {
    ReviewTarget {
        repository: RepositoryTarget {
            provider: RepositoryProvider::Gitlab,
            instance_base_url: "https://git.example:8443/forge".into(),
            project_path: project.into(),
        },
        kind,
        number,
    }
}

fn mr(project: &str, number: u64) -> ReviewTarget {
    target(project, RepositoryResourceKind::MergeRequest, number)
}
fn request<'a>(
    connection: &'a ConnectionObservations,
    target: &'a ReviewTarget,
) -> CacheRequest<'a> {
    CacheRequest {
        connection,
        target,
        revalidate: &|| Ok(()),
    }
}
fn quota() -> RateLimitStatus {
    RateLimitStatus {
        remaining: Some(0),
        reset_at: Some(2_000_000_099),
        limit: Some(100),
    }
}
fn denied(kind: ProviderFailureKind) -> intent_sourcecontrol::Error {
    intent_sourcecontrol::Error::Provider(ProviderFailure {
        kind,
        status: Some(403),
    })
}

fn observation(number: u64, title: &str) -> ReviewObservation {
    ReviewObservation {
        details: ReviewDetails {
            review: PullRequest {
                number,
                url: format!("https://git.example:8443/forge/team/app/-/merge_requests/{number}"),
                title: title.into(),
                body: None,
                state: PrState::Open,
                draft: false,
                source_branch: "feature".into(),
                target_branch: "main".into(),
                author: "alice".into(),
                mergeable: Some(true),
                mergeable_state: Some("clean".into()),
                head_sha: Some("abc".into()),
                created_at: String::new(),
                updated_at: "2026-09-27T12:00:00Z".into(),
            },
            source: Some(ReviewBranchIdentity {
                instance_base_url: "https://git.example:8443/forge".into(),
                project_id: u64::MAX,
                project_path: Some("team/app".into()),
                branch: "feature".into(),
            }),
            target: None,
            confirmed_draft: None,
            confirmed_state: Some(ConfirmedReviewState::Locked),
        },
        signals: MergeRequirementSignals {
            checks_known: true,
            ..Default::default()
        },
        reviews: Some(vec![]),
        threads: Some(ReviewThreadTally::default()),
        conversation_count: Some(4),
        availability: ReviewAvailability {
            policy: ProviderAvailability::Available,
            approvals: ProviderAvailability::Available,
            checks: ProviderAvailability::Available,
            discussions: ProviderAvailability::Available,
        },
    }
}

fn issue(number: u64, title: &str) -> Issue {
    Issue {
        number,
        title: title.into(),
        body: None,
        state: "open".into(),
        url: "https://git.example/issue".into(),
        author: "alice".into(),
        created_at: String::new(),
        updated_at: String::new(),
    }
}

async fn no_primary() -> ProviderRead<ReviewDetails> {
    panic!("a full read or cache hit needs no primary-only request")
}
async fn review(
    cache: &PrCache,
    req: &CacheRequest<'_>,
    max_age: Duration,
    result: intent_sourcecontrol::Result<ReviewObservation>,
) -> Result<CacheRead<ReviewObservation>, CacheFailure> {
    read_review(
        cache,
        req,
        PrReadPolicy::Serve { max_age },
        &NONE,
        no_primary,
        || async { (result, quota()) },
    )
    .await
}
async fn warm(cache: &PrCache, req: &CacheRequest<'_>, title: &str) {
    assert!(
        review(
            cache,
            req,
            Duration::ZERO,
            Ok(observation(req.target.number, title))
        )
        .await
        .unwrap()
        .fetched
    );
}
async fn hit(
    cache: &PrCache,
    req: &CacheRequest<'_>,
) -> Result<CacheRead<ReviewObservation>, CacheFailure> {
    review(
        cache,
        req,
        Duration::from_secs(60),
        Err(denied(ProviderFailureKind::Transient)),
    )
    .await
}
async fn issue_read(
    cache: &IssueCache,
    req: &CacheRequest<'_>,
    age: Duration,
    result: intent_sourcecontrol::Result<Issue>,
) -> Result<CacheRead<Issue>, CacheFailure> {
    read_qualified_issue(cache, req, age, || async { (result, quota()) }).await
}

#[tokio::test]
async fn qualified_hits_preserve_actual_provider_metadata_and_never_use_legacy_slots() {
    let reviews = PrCache::default();
    let issues = IssueCache::default();
    let conn = connection("alice");
    let review_target = mr("team/app", 7);
    let req = request(&conn, &review_target);
    warm(&reviews, &req, "qualified").await;
    let cached = hit(&reviews, &req).await.unwrap();
    assert!(!cached.fetched);
    assert_eq!(cached.value, observation(7, "qualified"));
    assert_eq!(cached.quota, quota());
    assert!(super::super::cached_pr_within(
        &reviews,
        &("team".into(), "app".into(), 7),
        Duration::from_secs(60)
    )
    .is_none());

    let issue_target = target("team/app", RepositoryResourceKind::Issue, 7);
    issues.lock().unwrap().insert(
        CacheKey::Legacy(("team".into(), "app".into(), 7)),
        CacheSlot::Legacy(IssueCacheEntry {
            issue: issue(7, "private legacy"),
            fetched_at: Instant::now(),
        }),
    );
    let req = request(&conn, &issue_target);
    let first = issue_read(
        &issues,
        &req,
        Duration::from_secs(60),
        Ok(issue(7, "qualified issue")),
    )
    .await
    .unwrap();
    assert!(first.fetched, "legacy warmth is not qualified authority");
    assert_eq!(first.value.title, "qualified issue");
    assert_eq!(
        issues.lock().unwrap().len(),
        2,
        "one map, disjoint representations"
    );
    let second = issue_read(
        &issues,
        &req,
        Duration::from_secs(60),
        Err(denied(ProviderFailureKind::Transient)),
    )
    .await
    .unwrap();
    assert!(!second.fetched);
}

#[tokio::test]
async fn full_canonical_identity_and_connection_lifetimes_separate_cache_entries() {
    let cache = PrCache::default();
    let conn = connection("alice");
    let base = mr("team/sub/app", 7);
    warm(&cache, &request(&conn, &base), "base").await;
    let mut variants = vec![];
    let mut changed = base.clone();
    changed.repository.instance_base_url = "https://git.example:9443/forge".into();
    variants.push(changed);
    let mut changed = base.clone();
    changed.repository.instance_base_url = "https://git.example:8443/other".into();
    variants.push(changed);
    let mut changed = base.clone();
    changed.repository.project_path = "other/sub/app".into();
    variants.push(changed);
    let mut changed = base.clone();
    changed.repository.provider = RepositoryProvider::Github;
    changed.kind = RepositoryResourceKind::PullRequest;
    variants.push(changed);
    for variant in variants {
        assert!(hit(&cache, &request(&conn, &variant)).await.is_err());
    }
    assert!(hit(&cache, &request(&connection("bob"), &base))
        .await
        .is_err());
    let execution = conn.key(base.clone()).scope.0;
    let other_authority = ConnectionObservations::new(
        ExecutionScope {
            authority_generation: execution.authority_generation + 1,
            ..execution
        },
        conn.key(base.clone()).scope.1,
    );
    assert!(hit(&cache, &request(&other_authority, &base))
        .await
        .is_err());
    assert_eq!(
        hit(&cache, &request(&conn, &base))
            .await
            .unwrap()
            .value
            .details
            .review
            .title,
        "base"
    );
}

async fn denial_breadth(kind: ProviderFailureKind, same_project: bool, other_project: bool) {
    let cache = PrCache::default();
    let issues = IssueCache::default();
    let conn = connection("alice");
    let unrelated = connection("bob");
    let addressed = mr("team/app", 7);
    let sibling = mr("team/app", 8);
    let elsewhere = mr("other/app", 7);
    let issue_target = target("team/app", RepositoryResourceKind::Issue, 7);
    for (c, t) in [
        (&conn, &addressed),
        (&conn, &sibling),
        (&conn, &elsewhere),
        (&unrelated, &addressed),
    ] {
        warm(&cache, &request(c, t), "old").await;
    }
    issue_read(
        &issues,
        &request(&conn, &issue_target),
        Duration::ZERO,
        Ok(issue(7, "old issue")),
    )
    .await
    .unwrap();
    let failure = review(
        &cache,
        &request(&conn, &addressed),
        Duration::ZERO,
        Err(denied(kind)),
    )
    .await
    .unwrap_err();
    assert!(matches!(failure.cause, CacheError::Provider(_)));
    assert_eq!(failure.quota, quota());
    assert!(hit(&cache, &request(&conn, &addressed)).await.is_err());
    assert_eq!(
        hit(&cache, &request(&conn, &sibling)).await.is_err(),
        same_project
    );
    assert_eq!(
        hit(&cache, &request(&conn, &elsewhere)).await.is_err(),
        other_project
    );
    assert!(hit(&cache, &request(&unrelated, &addressed)).await.is_ok());
    assert_eq!(
        issue_read(
            &issues,
            &request(&conn, &issue_target),
            Duration::from_secs(60),
            Err(denied(ProviderFailureKind::Transient))
        )
        .await
        .is_err(),
        same_project
    );
    warm(&cache, &request(&conn, &addressed), "recovered").await;
    assert_eq!(
        hit(&cache, &request(&conn, &addressed))
            .await
            .unwrap()
            .value
            .details
            .review
            .title,
        "recovered"
    );
    assert_eq!(
        hit(&cache, &request(&conn, &sibling)).await.is_err(),
        same_project,
        "one fresh item does not restore other items"
    );
    assert_eq!(
        issue_read(
            &issues,
            &request(&conn, &issue_target),
            Duration::from_secs(60),
            Err(denied(ProviderFailureKind::Transient))
        )
        .await
        .is_err(),
        same_project
    );
}

#[tokio::test]
async fn credential_rejection_invalidates_both_real_caches_only_in_its_connection() {
    denial_breadth(ProviderFailureKind::CredentialRejected, true, true).await;
}
#[tokio::test]
async fn project_denial_covers_same_project_reviews_and_issues_only() {
    denial_breadth(ProviderFailureKind::ProjectDenied, true, false).await;
}
#[tokio::test]
async fn item_denial_does_not_clear_sibling_reviews_or_same_number_issue() {
    denial_breadth(ProviderFailureKind::ResourceDenied, false, false).await;
}

#[tokio::test]
async fn issue_denial_fences_all_affected_in_flight_results_even_after_fresh_recovery() {
    for kind in [
        ProviderFailureKind::CredentialRejected,
        ProviderFailureKind::ProjectDenied,
    ] {
        let reviews = PrCache::default();
        let issues = IssueCache::default();
        let conn = connection("alice");
        let change = mr("team/app", 7);
        let issue_target = target("team/app", RepositoryResourceKind::Issue, 7);
        let review_req = request(&conn, &change);
        let issue_req = request(&conn, &issue_target);
        let (arrive_pr, seen_pr) = oneshot::channel();
        let (release_pr, wait_pr) = oneshot::channel();
        let (arrive_issue, seen_issue) = oneshot::channel();
        let (release_issue, wait_issue) = oneshot::channel();
        let late_pr = read_review(
            &reviews,
            &review_req,
            PrReadPolicy::REFRESH,
            &NONE,
            no_primary,
            || async {
                arrive_pr.send(()).unwrap();
                wait_pr.await.unwrap();
                (Ok(observation(7, "late")), quota())
            },
        );
        let late_issue = read_qualified_issue(&issues, &issue_req, Duration::ZERO, || async {
            arrive_issue.send(()).unwrap();
            wait_issue.await.unwrap();
            (Ok(issue(7, "late")), quota())
        });
        let action = async {
            seen_pr.await.unwrap();
            seen_issue.await.unwrap();
            issue_read(&issues, &issue_req, Duration::ZERO, Err(denied(kind)))
                .await
                .unwrap_err();
            warm(&reviews, &review_req, "fresh").await;
            release_pr.send(()).unwrap();
            release_issue.send(()).unwrap();
        };
        let (late_pr, late_issue, ()) = tokio::join!(late_pr, late_issue, action);
        assert!(matches!(
            late_pr.unwrap_err().cause,
            CacheError::Ineligible(_)
        ));
        assert!(matches!(
            late_issue.unwrap_err().cause,
            CacheError::Ineligible(_)
        ));
        assert_eq!(
            hit(&reviews, &review_req)
                .await
                .unwrap()
                .value
                .details
                .review
                .title,
            "fresh"
        );
        assert!(issue_read(
            &issues,
            &issue_req,
            Duration::from_secs(60),
            Err(denied(ProviderFailureKind::Transient))
        )
        .await
        .is_err());
    }
}

#[tokio::test]
async fn caller_authority_is_checked_on_hits_before_fetch_and_after_deferred_reads() {
    let cache = PrCache::default();
    let conn = connection("alice");
    let change = mr("team/app", 7);
    let authorized = AtomicBool::new(true);
    let admission = || {
        if authorized.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(intent_core::Error::Internal("caller authority lost".into()))
        }
    };
    let req = CacheRequest {
        connection: &conn,
        target: &change,
        revalidate: &admission,
    };
    warm(&cache, &req, "private").await;
    authorized.store(false, Ordering::SeqCst);
    for policy in [
        PrReadPolicy::REFRESH,
        PrReadPolicy::Serve {
            max_age: Duration::from_secs(60),
        },
    ] {
        let failure = read_review(&cache, &req, policy, &NONE, no_primary, || async {
            panic!("lost authority must not read the provider")
        })
        .await
        .unwrap_err();
        assert!(matches!(failure.cause, CacheError::Admission(_)));
    }
    authorized.store(true, Ordering::SeqCst);
    let result = read_review(
        &cache,
        &req,
        PrReadPolicy::REFRESH,
        &NONE,
        no_primary,
        || async {
            authorized.store(false, Ordering::SeqCst);
            (Ok(observation(7, "not admitted")), quota())
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(result.cause, CacheError::Admission(_)));
    assert_eq!(result.quota, quota());
    authorized.store(true, Ordering::SeqCst);
    assert_eq!(
        hit(&cache, &req).await.unwrap().value.details.review.title,
        "private"
    );
}

#[tokio::test]
async fn retired_lifetime_and_replacement_exclude_old_success_and_denial() {
    for deny in [false, true] {
        let cache = PrCache::default();
        let old = connection("alice");
        let replacement = connection("alice");
        let target = mr("team/app", 7);
        let req = request(&old, &target);
        let next = request(&replacement, &target);
        let (arrive, seen) = oneshot::channel();
        let (release, wait) = oneshot::channel();
        let late = read_review(
            &cache,
            &req,
            PrReadPolicy::REFRESH,
            &NONE,
            no_primary,
            || async {
                arrive.send(()).unwrap();
                wait.await.unwrap();
                (
                    if deny {
                        Err(denied(ProviderFailureKind::CredentialRejected))
                    } else {
                        Ok(observation(7, "old"))
                    },
                    quota(),
                )
            },
        );
        let replace = async {
            seen.await.unwrap();
            old.retire();
            warm(&cache, &next, "new lifetime").await;
            release.send(()).unwrap();
        };
        let (late, ()) = tokio::join!(late, replace);
        assert!(matches!(
            late.unwrap_err().cause,
            CacheError::Ineligible(Ineligible::RetiredScope)
        ));
        assert_eq!(
            hit(&cache, &next).await.unwrap().value.details.review.title,
            "new lifetime"
        );
    }
}

#[tokio::test]
async fn partial_and_optional_evidence_preserves_quota_without_refreshing_complete_cache() {
    let cache = PrCache::default();
    let conn = connection("alice");
    let change = mr("team/app", 7);
    let req = request(&conn, &change);
    warm(&cache, &req, "complete").await;
    let fetched_at = match cache.lock().unwrap().get(&req.key()).unwrap() {
        CacheSlot::Qualified(s) => s.payload.as_ref().unwrap().freshness.fetched_at,
        CacheSlot::Legacy(_) => unreachable!(),
    };
    for state in [
        ProviderAvailability::Restricted,
        ProviderAvailability::Unavailable,
        ProviderAvailability::Transient,
        ProviderAvailability::Unknown,
        ProviderAvailability::RateLimited,
    ] {
        let mut partial = observation(7, "partial");
        partial.availability.discussions = state;
        partial.threads = None;
        let returned = review(&cache, &req, Duration::ZERO, Ok(partial.clone()))
            .await
            .unwrap();
        assert_eq!(returned.value, partial);
        assert_eq!(returned.quota, quota());
        let still = hit(&cache, &req).await.unwrap();
        assert_eq!(still.value.details.review.title, "complete");
    }
    for error in [
        denied(ProviderFailureKind::OptionalRestricted),
        denied(ProviderFailureKind::Transient),
        denied(ProviderFailureKind::Unknown),
        intent_sourcecontrol::Error::RateLimited("original quota".into()),
    ] {
        let expected = format!("{error:?}");
        let failure = review(&cache, &req, Duration::ZERO, Err(error))
            .await
            .unwrap_err();
        let CacheError::Provider(error) = failure.cause else {
            panic!("provider classification lost")
        };
        assert_eq!(format!("{error:?}"), expected);
        assert_eq!(failure.quota, quota());
    }
    let guard = cache.lock().unwrap();
    let CacheSlot::Qualified(s) = guard.get(&req.key()).unwrap() else {
        unreachable!()
    };
    assert_eq!(s.payload.as_ref().unwrap().freshness.fetched_at, fetched_at);
}

#[tokio::test]
async fn partial_pre_denial_result_cannot_be_returned_or_used_to_recover() {
    let cache = PrCache::default();
    let conn = connection("alice");
    let target = mr("team/app", 7);
    let req = request(&conn, &target);
    let (arrive, seen) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    let late = read_review(
        &cache,
        &req,
        PrReadPolicy::REFRESH,
        &NONE,
        no_primary,
        || async {
            arrive.send(()).unwrap();
            wait.await.unwrap();
            let mut partial = observation(7, "old private");
            partial.reviews = None;
            (Ok(partial), quota())
        },
    );
    let action = async {
        seen.await.unwrap();
        review(
            &cache,
            &req,
            Duration::ZERO,
            Err(denied(ProviderFailureKind::ResourceDenied)),
        )
        .await
        .unwrap_err();
        release.send(()).unwrap();
    };
    let (late, ()) = tokio::join!(late, action);
    let failure = late.unwrap_err();
    assert!(matches!(
        failure.cause,
        CacheError::Ineligible(Ineligible::DeniedSinceRequest)
    ));
    assert_eq!(failure.quota, quota());
    assert!(hit(&cache, &req).await.is_err());
}

#[tokio::test]
async fn newer_on_demand_result_fences_a_deferred_cheap_poll_before_more_provider_reads() {
    let cache = PrCache::default();
    let conn = connection("alice");
    let target = mr("team/app", 7);
    let req = request(&conn, &target);
    warm(&cache, &req, "baseline").await;
    let (arrive, seen) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    let poll = read_review(
        &cache,
        &req,
        PrReadPolicy::Poll,
        &NONE,
        || async {
            arrive.send(()).unwrap();
            wait.await.unwrap();
            (Ok(observation(7, "old poll").details), quota())
        },
        || async { panic!("superseded primary must not start a full read") },
    );
    let action = async {
        seen.await.unwrap();
        warm(&cache, &req, "new hover").await;
        release.send(()).unwrap();
    };
    let (poll, ()) = tokio::join!(poll, action);
    assert!(matches!(
        poll.unwrap_err().cause,
        CacheError::Ineligible(Ineligible::OlderObservation)
    ));
    assert_eq!(
        hit(&cache, &req).await.unwrap().value.details.review.title,
        "new hover"
    );
}

#[tokio::test]
async fn a_failed_full_read_fences_an_in_flight_cheap_poll() {
    failed_full_read_fences_poll(denied(ProviderFailureKind::Transient)).await;
}

#[tokio::test]
async fn a_rate_limited_full_read_fences_an_in_flight_cheap_poll() {
    failed_full_read_fences_poll(intent_sourcecontrol::Error::RateLimited(
        "retry after the captured reset".into(),
    ))
    .await;
}

async fn failed_full_read_fences_poll(error: intent_sourcecontrol::Error) {
    let cache = PrCache::default();
    let conn = connection("alice");
    let target = mr("team/app", 7);
    let req = request(&conn, &target);
    warm(&cache, &req, "baseline").await;
    let old_freshness = {
        let mut rows = cache.lock().unwrap();
        let CacheSlot::Qualified(slot) = rows.get_mut(&req.key()).unwrap() else {
            unreachable!()
        };
        let freshness = &mut slot.payload.as_mut().unwrap().freshness;
        freshness.fetched_at -= Duration::from_secs(2);
        freshness.refreshed_at -= Duration::from_secs(2);
        *freshness
    };
    let unchanged = || {
        let rows = cache.lock().unwrap();
        let CacheSlot::Qualified(slot) = rows.get(&req.key()).unwrap() else {
            unreachable!()
        };
        let payload = slot.payload.as_ref().unwrap();
        assert_eq!(payload.freshness.fetched_at, old_freshness.fetched_at);
        assert_eq!(payload.freshness.refreshed_at, old_freshness.refreshed_at);
        assert_eq!(payload.freshness.cheap_polls, old_freshness.cheap_polls);
        assert_eq!(payload.value.details.review.title, "baseline");
        assert!(!slot.reuse_allowed);
    };
    let (arrive, seen) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    let poll = read_review(
        &cache,
        &req,
        PrReadPolicy::Poll,
        &NONE,
        || async {
            arrive.send(()).unwrap();
            wait.await.unwrap();
            (Ok(observation(7, "old poll").details), quota())
        },
        || async { panic!("the superseded cheap result is rejected before a full read") },
    );
    let newer = async {
        seen.await.unwrap();
        let expected = format!("{error:?}");
        let failure = review(&cache, &req, Duration::ZERO, Err(error))
            .await
            .unwrap_err();
        let CacheError::Provider(original) = failure.cause else {
            panic!("the original typed provider error must survive")
        };
        assert_eq!(format!("{original:?}"), expected);
        assert_eq!(failure.quota, quota());
        unchanged();
        let hit = hit(&cache, &req).await.unwrap();
        assert!(!hit.fetched);
        assert_eq!(hit.value.details.review.title, "baseline");
        release.send(()).unwrap();
    };
    let (poll, ()) = tokio::join!(poll, newer);
    let rejected = poll.unwrap_err();
    assert!(matches!(
        rejected.cause,
        CacheError::Ineligible(Ineligible::OlderObservation)
    ));
    assert_eq!(rejected.quota, quota());
    unchanged();

    warm(&cache, &req, "fresh complete").await;
    {
        let rows = cache.lock().unwrap();
        let CacheSlot::Qualified(slot) = rows.get(&req.key()).unwrap() else {
            unreachable!()
        };
        let payload = slot.payload.as_ref().unwrap();
        assert!(slot.reuse_allowed);
        assert!(payload.freshness.fetched_at > old_freshness.fetched_at);
        assert_eq!(payload.freshness.cheap_polls, 0);
    }
    assert_eq!(
        hit(&cache, &req).await.unwrap().value.details.review.title,
        "fresh complete"
    );
    let recovered = read_review(
        &cache,
        &req,
        PrReadPolicy::Poll,
        &NONE,
        || async { (Ok(observation(7, "fresh poll").details), quota()) },
        || async { panic!("the fresh full read permits cheap polling again") },
    )
    .await
    .unwrap();
    assert_eq!(recovered.value.details.review.title, "fresh poll");
    assert_eq!(recovered.quota, quota());
}

#[tokio::test]
async fn cheap_polls_keep_completeness_count_and_age_limits_and_confirmed_metadata() {
    let cache = PrCache::default();
    let conn = connection("alice");
    let target = mr("team/app", 7);
    let req = request(&conn, &target);
    warm(&cache, &req, "baseline").await;
    let full_reads = AtomicUsize::new(0);
    for _ in 0..=PR_MONITOR_MAX_CHEAP_POLLS {
        read_review(
            &cache,
            &req,
            PrReadPolicy::Poll,
            &NONE,
            || async { (Ok(observation(7, "polled").details), quota()) },
            || async {
                full_reads.fetch_add(1, Ordering::SeqCst);
                (Ok(observation(7, "full")), quota())
            },
        )
        .await
        .unwrap();
    }
    assert_eq!(full_reads.load(Ordering::SeqCst), 1);
    {
        let mut cache = cache.lock().unwrap();
        let CacheSlot::Qualified(s) = cache.get_mut(&req.key()).unwrap() else {
            unreachable!()
        };
        s.payload.as_mut().unwrap().freshness.fetched_at -= PR_MONITOR_MAX_CHEAP_AGE;
    }
    read_review(
        &cache,
        &req,
        PrReadPolicy::Poll,
        &NONE,
        || async { (Ok(observation(7, "age").details), quota()) },
        || async {
            full_reads.fetch_add(1, Ordering::SeqCst);
            (Ok(observation(7, "age refresh")), quota())
        },
    )
    .await
    .unwrap();
    assert_eq!(full_reads.load(Ordering::SeqCst), 2);
    assert_eq!(
        hit(&cache, &req)
            .await
            .unwrap()
            .value
            .details
            .confirmed_draft,
        None
    );
}

#[tokio::test]
async fn bounded_eviction_and_reinsertion_fence_old_success_and_typed_denial() {
    for deny in [false, true] {
        let cache = PrCache::default();
        let conn = connection("alice");
        let target = mr("team/app", 7);
        let req = request(&conn, &target);
        let (arrive, seen) = oneshot::channel();
        let (release, wait) = oneshot::channel();
        let late = read_review(
            &cache,
            &req,
            PrReadPolicy::REFRESH,
            &NONE,
            no_primary,
            || async {
                arrive.send(()).unwrap();
                wait.await.unwrap();
                (
                    if deny {
                        Err(denied(ProviderFailureKind::ProjectDenied))
                    } else {
                        Ok(observation(7, "evicted"))
                    },
                    quota(),
                )
            },
        );
        let action = async {
            seen.await.unwrap();
            for n in 100..100 + PR_CACHE_MAX_ENTRIES as u64 {
                warm(&cache, &request(&conn, &mr("team/app", n)), "bounded").await;
                assert!(cache.lock().unwrap().len() <= PR_CACHE_MAX_ENTRIES);
            }
            assert!(!cache.lock().unwrap().contains_key(&req.key()));
            warm(&cache, &req, "reinserted").await;
            release.send(()).unwrap();
        };
        let (late, ()) = tokio::join!(late, action);
        assert!(matches!(
            late.unwrap_err().cause,
            CacheError::Ineligible(Ineligible::DifferentSlot)
        ));
        assert_eq!(
            hit(&cache, &req).await.unwrap().value.details.review.title,
            "reinserted"
        );
    }
}

#[tokio::test]
async fn a_partial_full_attempt_forces_the_next_poll_to_retry_optional_fields() {
    let cache = PrCache::default();
    let conn = connection("alice");
    let target = mr("team/app", 7);
    let req = request(&conn, &target);
    warm(&cache, &req, "previous complete").await;
    let mut partial = observation(7, "partial");
    partial.availability.discussions = ProviderAvailability::Restricted;
    partial.threads = None;
    review(&cache, &req, Duration::ZERO, Ok(partial))
        .await
        .unwrap();
    let full_reads = AtomicUsize::new(0);
    let refreshed = read_review(
        &cache,
        &req,
        PrReadPolicy::Poll,
        &NONE,
        || async { (Ok(observation(7, "same fingerprint").details), quota()) },
        || async {
            full_reads.fetch_add(1, Ordering::SeqCst);
            (Ok(observation(7, "retried fields")), quota())
        },
    )
    .await
    .unwrap();
    assert_eq!(
        full_reads.load(Ordering::SeqCst),
        1,
        "a newer partial read disables reuse of the older complete fields"
    );
    assert_eq!(refreshed.value.details.review.title, "retried fields");
}

#[tokio::test]
async fn a_newer_partial_result_fences_an_older_complete_read_without_refreshing_the_cache() {
    let cache = PrCache::default();
    let conn = connection("alice");
    let target = mr("team/app", 7);
    let req = request(&conn, &target);
    warm(&cache, &req, "baseline").await;
    let (arrive, seen) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    let older = read_review(
        &cache,
        &req,
        PrReadPolicy::REFRESH,
        &NONE,
        no_primary,
        || async {
            arrive.send(()).unwrap();
            wait.await.unwrap();
            (Ok(observation(7, "older complete")), quota())
        },
    );
    let newer = async {
        seen.await.unwrap();
        let mut partial = observation(7, "new restriction");
        partial.availability.discussions = ProviderAvailability::Restricted;
        partial.threads = None;
        review(&cache, &req, Duration::ZERO, Ok(partial))
            .await
            .unwrap();
        release.send(()).unwrap();
    };
    let (older, ()) = tokio::join!(older, newer);
    assert!(matches!(
        older.unwrap_err().cause,
        CacheError::Ineligible(Ineligible::OlderObservation)
    ));
    assert_eq!(
        hit(&cache, &req).await.unwrap().value.details.review.title,
        "baseline"
    );
    let full_reads = AtomicUsize::new(0);
    read_review(
        &cache,
        &req,
        PrReadPolicy::Poll,
        &NONE,
        || async { (Ok(observation(7, "unchanged").details), quota()) },
        || async {
            full_reads.fetch_add(1, Ordering::SeqCst);
            (Ok(observation(7, "fresh complete")), quota())
        },
    )
    .await
    .unwrap();
    assert_eq!(full_reads.load(Ordering::SeqCst), 1);
    assert_eq!(
        hit(&cache, &req).await.unwrap().value.details.review.title,
        "fresh complete"
    );
}

#[tokio::test]
async fn a_cold_partial_does_not_seed_a_hit_or_a_cheap_poll() {
    let cache = PrCache::default();
    let conn = connection("alice");
    let target = mr("team/app", 7);
    let req = request(&conn, &target);
    let mut partial = observation(7, "partial");
    partial.signals.checks_known = false;
    partial.availability.checks = ProviderAvailability::Unknown;
    review(&cache, &req, Duration::ZERO, Ok(partial))
        .await
        .unwrap();
    assert!(hit(&cache, &req).await.is_err());
    let full_reads = AtomicUsize::new(0);
    read_review(
        &cache,
        &req,
        PrReadPolicy::Poll,
        &NONE,
        || async { (Ok(observation(7, "same fingerprint").details), quota()) },
        || async {
            full_reads.fetch_add(1, Ordering::SeqCst);
            (Ok(observation(7, "retried fields")), quota())
        },
    )
    .await
    .unwrap();
    assert_eq!(full_reads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn issue_eviction_and_reinsertion_excludes_a_pre_eviction_completion() {
    let cache = IssueCache::default();
    let conn = connection("alice");
    let target = target("team/app", RepositoryResourceKind::Issue, 7);
    let req = request(&conn, &target);
    let (arrive, seen) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    let late = read_qualified_issue(&cache, &req, Duration::ZERO, || async {
        arrive.send(()).unwrap();
        wait.await.unwrap();
        (Ok(issue(7, "evicted")), quota())
    });
    let action = async {
        seen.await.unwrap();
        for number in 100..100 + PR_CACHE_MAX_ENTRIES as u64 {
            let resource = self::target("team/app", RepositoryResourceKind::Issue, number);
            issue_read(
                &cache,
                &request(&conn, &resource),
                Duration::ZERO,
                Ok(issue(number, "bounded")),
            )
            .await
            .unwrap();
            assert!(cache.lock().unwrap().len() <= PR_CACHE_MAX_ENTRIES);
        }
        assert!(!cache.lock().unwrap().contains_key(&req.key()));
        issue_read(&cache, &req, Duration::ZERO, Ok(issue(7, "reinserted")))
            .await
            .unwrap();
        release.send(()).unwrap();
    };
    let (late, ()) = tokio::join!(late, action);
    assert!(matches!(
        late.unwrap_err().cause,
        CacheError::Ineligible(Ineligible::DifferentSlot)
    ));
    let cached = issue_read(
        &cache,
        &req,
        Duration::from_secs(60),
        Err(denied(ProviderFailureKind::Transient)),
    )
    .await
    .unwrap();
    assert_eq!(cached.value.title, "reinserted");
}

#[tokio::test]
async fn review_idle_expiry_allows_the_first_fresh_read_to_replace_the_expired_slot() {
    let cache = PrCache::default();
    let conn = connection("alice");
    let target = mr("team/app", 7);
    let req = request(&conn, &target);
    warm(&cache, &req, "expired").await;
    {
        let mut rows = cache.lock().unwrap();
        let CacheSlot::Qualified(row) = rows.get_mut(&req.key()).unwrap() else {
            unreachable!()
        };
        row.payload.as_mut().unwrap().freshness.fetched_at -= PR_CACHE_MAX_IDLE;
        row.payload.as_mut().unwrap().freshness.refreshed_at -= PR_CACHE_MAX_IDLE;
    }
    let refreshed = review(
        &cache,
        &req,
        Duration::from_secs(60),
        Ok(observation(7, "fresh")),
    )
    .await
    .unwrap();
    assert!(refreshed.fetched);
    assert_eq!(
        hit(&cache, &req).await.unwrap().value.details.review.title,
        "fresh"
    );
}

#[tokio::test]
async fn issue_idle_expiry_allows_the_first_fresh_read_to_replace_the_expired_slot() {
    let cache = IssueCache::default();
    let conn = connection("alice");
    let target = target("team/app", RepositoryResourceKind::Issue, 7);
    let req = request(&conn, &target);
    issue_read(&cache, &req, Duration::ZERO, Ok(issue(7, "expired")))
        .await
        .unwrap();
    {
        let mut rows = cache.lock().unwrap();
        let CacheSlot::Qualified(row) = rows.get_mut(&req.key()).unwrap() else {
            unreachable!()
        };
        row.payload.as_mut().unwrap().freshness.fetched_at -= PR_CACHE_MAX_IDLE;
        row.payload.as_mut().unwrap().freshness.refreshed_at -= PR_CACHE_MAX_IDLE;
    }
    let refreshed = issue_read(&cache, &req, Duration::from_secs(60), Ok(issue(7, "fresh")))
        .await
        .unwrap();
    assert!(refreshed.fetched);
    let cached = issue_read(
        &cache,
        &req,
        Duration::from_secs(60),
        Err(denied(ProviderFailureKind::Transient)),
    )
    .await
    .unwrap();
    assert!(!cached.fetched);
    assert_eq!(cached.value.title, "fresh");
}

#[tokio::test]
async fn qualified_and_legacy_rows_share_caps_and_monitor_exemptions_survive_legacy_pruning() {
    let cache = PrCache::default();
    let conn = connection("alice");
    let target = mr("team/app", 7);
    let req = request(&conn, &target);
    warm(&cache, &req, "monitored").await;
    {
        let mut rows = cache.lock().unwrap();
        let CacheSlot::Qualified(row) = rows.get_mut(&req.key()).unwrap() else {
            unreachable!()
        };
        row.monitored = true;
        row.payload.as_mut().unwrap().freshness.fetched_at -= PR_CACHE_MAX_IDLE;
    }
    for n in 100..100 + PR_CACHE_MAX_ENTRIES as u64 + 3 {
        warm(&cache, &request(&conn, &mr("team/app", n)), "bounded").await;
    }
    super::super::prune_pr_cache(&cache, &NONE);
    assert_eq!(cache.lock().unwrap().len(), PR_CACHE_MAX_ENTRIES + 1);
    assert!(cache.lock().unwrap().contains_key(&req.key()));
    {
        let mut rows = cache.lock().unwrap();
        let CacheSlot::Qualified(row) = rows.get_mut(&req.key()).unwrap() else {
            unreachable!()
        };
        row.monitored = false;
    }
    super::super::prune_pr_cache(&cache, &NONE);
    assert_eq!(cache.lock().unwrap().len(), PR_CACHE_MAX_ENTRIES);

    let issues = IssueCache::default();
    for n in 0..PR_CACHE_MAX_ENTRIES as u64 {
        issues.lock().unwrap().insert(
            CacheKey::Legacy(("team".into(), "app".into(), n)),
            CacheSlot::Legacy(IssueCacheEntry {
                issue: issue(n, "legacy"),
                fetched_at: Instant::now().checked_sub(Duration::from_secs(1)).unwrap(),
            }),
        );
    }
    let target = self::target("team/app", RepositoryResourceKind::Issue, 7);
    issue_read(
        &issues,
        &request(&conn, &target),
        Duration::ZERO,
        Ok(issue(7, "qualified")),
    )
    .await
    .unwrap();
    assert_eq!(
        issues.lock().unwrap().len(),
        PR_CACHE_MAX_ENTRIES,
        "one combined bound"
    );
}
