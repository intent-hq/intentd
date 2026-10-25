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

// Real original owner/file/HTTP composition. Caller authority is explicitly
// injected; this suite does not install an RPC or final-delivery authority.
mod managed {
    use super::*;
    use crate::repository_credentials::authority::{
        CredentialFuture, RepositoryAuthority, RepositoryAuthorityFence,
        RepositoryAuthorityRequest, RepositoryCredentialTransport,
    };
    use crate::repository_credentials::read::RepositoryReadOperation;
    use crate::repository_credentials::{
        RepositoryCredentialAdmission, RepositoryCredentialError as Error, RepositoryCredentialUse,
        Result,
    };
    use crate::source_control_auth_ops::repository_owner::secret_reader::tests::{Fixture, Server};
    use crate::source_control_auth_ops::repository_owner::RepositoryReadEligibility;
    use intent_sourcecontrol::{
        gitlab_token::{EXPIRES_AT_SECRET_ACCOUNT, REFRESH_SECRET_ACCOUNT},
        GitlabDescriptor,
    };
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::Notify;
    use tokio::time::timeout;
    const BUDGET: Duration = Duration::from_secs(5);
    const MR: &str = "/api/v4/projects/group%2Fproject/merge_requests/4";
    const PROJECT: &str = "/api/v4/projects/group%2Fproject";
    const ISSUE: &str = "/api/v4/projects/group%2Fproject/issues/4";

    #[derive(Default)]
    struct InjectedAuthority {
        denied: AtomicBool,
        calls: AtomicUsize,
    }
    impl RepositoryAuthority for InjectedAuthority {
        fn revalidate<'a>(
            &'a self,
            _: &'a RepositoryAuthorityRequest,
        ) -> CredentialFuture<'a, Box<dyn RepositoryAuthorityFence>> {
            Box::pin(async {
                self.calls.fetch_add(1, Ordering::SeqCst);
                if self.denied.load(Ordering::SeqCst) {
                    return Err(Error::AuthorityDenied);
                }
                Ok(Box::new(InjectedFence) as Box<dyn RepositoryAuthorityFence>)
            })
        }
    }
    struct InjectedFence;
    impl RepositoryAuthorityFence for InjectedFence {
        fn dispatch(
            self: Box<Self>,
            action: &mut (dyn FnMut() -> Result<()> + Send),
        ) -> Result<()> {
            action()
        }
    }

    #[derive(Default)]
    struct Replies {
        statuses: Mutex<HashMap<String, u16>>,
        pause: Mutex<Option<String>>,
        entered: Notify,
        release: Notify,
        calls: Mutex<Vec<String>>,
    }
    struct ReadServer {
        fixture: Server,
        replies: Arc<Replies>,
        task: tokio::task::JoinHandle<()>,
    }
    impl Drop for ReadServer {
        fn drop(&mut self) {
            self.replies.release.notify_waiters();
            self.task.abort();
        }
    }
    impl ReadServer {
        async fn new() -> Self {
            let mut fixture = Server::new().await;
            let upstream = fixture
                .host
                .base_url()
                .trim_start_matches("http://")
                .trim_end_matches('/')
                .to_owned();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            // Only select the disposable endpoint BEFORE original Services creation.
            // Auth/adoption still forwards to the unchanged original owner fixture.
            fixture.host = fixture.host.clone().with_api_origin(&endpoint).unwrap();
            fixture.descriptor = GitlabDescriptor::with_loopback_endpoint(
                fixture.descriptor.instance().clone(),
                &endpoint,
            )
            .unwrap();
            let replies = Arc::new(Replies::default());
            let control = replies.clone();
            let task = tokio::spawn(async move {
                loop {
                    let Ok((mut socket, _)) = listener.accept().await else {
                        return;
                    };
                    let control = control.clone();
                    let upstream = upstream.clone();
                    tokio::spawn(async move {
                        let mut request = Vec::new();
                        let mut buf = [0_u8; 4096];
                        loop {
                            let Ok(n) = socket.read(&mut buf).await else {
                                return;
                            };
                            if n == 0 {
                                return;
                            }
                            request.extend_from_slice(&buf[..n]);
                            assert!(request.len() < 65_536);
                            if let Some(end) = request.windows(4).position(|b| b == b"\r\n\r\n") {
                                let header = String::from_utf8_lossy(&request[..end]);
                                let len = header
                                    .lines()
                                    .find_map(|line| {
                                        let (k, v) = line.split_once(':')?;
                                        k.eq_ignore_ascii_case("content-length")
                                            .then(|| v.trim().parse::<usize>().unwrap())
                                    })
                                    .unwrap_or(0);
                                if request.len() >= end + 4 + len {
                                    break;
                                }
                            }
                        }
                        let text = String::from_utf8_lossy(&request);
                        let path = text.split_whitespace().nth(1).unwrap().to_owned();
                        if !path.starts_with("/api/v4/projects/") {
                            let mut remote =
                                tokio::net::TcpStream::connect(&upstream).await.unwrap();
                            remote.write_all(&request).await.unwrap();
                            let mut response = Vec::new();
                            remote.read_to_end(&mut response).await.unwrap();
                            let _ = socket.write_all(&response).await;
                            return;
                        }
                        assert!(
                            text.contains("stored-pat")
                                || text.contains("rotated")
                                || text.contains("pat-second"),
                            "only real original-owner tokens reach this fixture"
                        );
                        control.calls.lock().unwrap().push(path.clone());
                        let endpoint = path.split('?').next().unwrap();
                        let status = {
                            let statuses = control.statuses.lock().unwrap();
                            *statuses
                                .get(&path)
                                .or_else(|| statuses.get(endpoint))
                                .unwrap_or(&200)
                        };
                        let pause = {
                            let mut pause = control.pause.lock().unwrap();
                            if pause.as_ref().is_some_and(|p| p == &path || p == endpoint) {
                                pause.take();
                                true
                            } else {
                                false
                            }
                        };
                        if pause {
                            control.entered.notify_one();
                            control.release.notified().await;
                        }
                        let body=if endpoint.ends_with("/merge_requests") {
                        json!([{"iid":4,"web_url":"https://gitlab.test/forge/group/project/-/merge_requests/4",
                            "title":"listed review","state":"opened","draft":false,"source_branch":"feature","target_branch":"main",
                            "source_project_id":42,"target_project_id":42,"created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z"}])
                    } else if endpoint.ends_with("/issues") {
                        json!([{"iid":4,"web_url":"https://gitlab.test/forge/group/project/-/issues/4",
                            "title":"listed issue","state":"opened","created_at":"2026-10-13T00:00:00Z","updated_at":"2026-10-13T00:00:00Z"}])
                    } else if path==PROJECT {
                        json!({"id":42,"path_with_namespace":"group/project",
                            "only_allow_merge_if_pipeline_succeeds":false,
                            "only_allow_merge_if_all_discussions_are_resolved":false})
                    } else if path.ends_with("/approvals") {
                        json!({"approvals_required":0,"approvals_left":0,"approved_by":[]})
                    } else if path.contains("/discussions") { json!([]) }
                    else if path==MR {
                        json!({"iid":4,"web_url":"https://gitlab.test/forge/group/project/-/merge_requests/4",
                            "title":"actual review","state":"opened","draft":false,"source_branch":"feature","target_branch":"main",
                            "source_project_id":42,"target_project_id":42,"created_at":"2026-09-27T00:00:00Z","updated_at":"2026-09-27T00:00:00Z"})
                    } else {
                        json!({"iid":4,"web_url":"https://gitlab.test/forge/group/project/-/issues/4",
                            "title":"actual issue","state":"opened","created_at":"2026-09-27T00:00:00Z","updated_at":"2026-09-27T00:00:00Z"})
                    }.to_string();
                        let extra = if status == 429 {
                            "Retry-After: 60\r\n"
                        } else {
                            ""
                        };
                        let response=format!("HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nRateLimit-Remaining: 23\r\nRateLimit-Reset: 4000000000\r\n{extra}Connection: close\r\n\r\n{body}",body.len());
                        let _ = socket.write_all(response.as_bytes()).await;
                    });
                }
            });
            Self {
                fixture,
                replies,
                task,
            }
        }
        fn status(&self, path: &str, status: u16) {
            self.replies
                .statuses
                .lock()
                .unwrap()
                .insert(path.into(), status);
        }
        fn pause(&self, path: &str) {
            *self.replies.pause.lock().unwrap() = Some(path.into());
        }
        async fn entered(&self) {
            timeout(
                intentd_test_support::test_timeout(BUDGET),
                self.replies.entered.notified(),
            )
            .await
            .unwrap();
        }
        fn resume(&self) {
            self.replies.release.notify_one();
        }
        fn count(&self) -> usize {
            self.replies.calls.lock().unwrap().len()
        }
    }

    struct ReadFixture {
        auth: Fixture,
        authority: Arc<InjectedAuthority>,
    }
    impl ReadFixture {
        async fn new(server: &ReadServer, oauth: bool) -> Self {
            let auth = if oauth {
                let f = Fixture::unadopted(&server.fixture).await;
                f.service
                    .gitlab_secret_store
                    .store(REFRESH_SECRET_ACCOUNT, "refresh-old")
                    .unwrap();
                let expiry = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 7200;
                f.service
                    .gitlab_secret_store
                    .store(EXPIRES_AT_SECRET_ACCOUNT, &expiry.to_string())
                    .unwrap();
                f.service
                    .reconcile_gitlab_repository_binding()
                    .await
                    .unwrap();
                f
            } else {
                Fixture::new(&server.fixture).await
            };
            Self {
                auth,
                authority: Arc::new(InjectedAuthority::default()),
            }
        }
        fn target(&self, kind: RepositoryResourceKind) -> ReviewTarget {
            ReviewTarget {
                repository: RepositoryTarget {
                    provider: RepositoryProvider::Gitlab,
                    instance_base_url: self.auth.request().binding.account.instance_base_url,
                    project_path: "group/project".into(),
                },
                kind,
                number: 4,
            }
        }
        fn admission(&self, server: &ReadServer) -> RepositoryCredentialAdmission {
            let directory = self.auth.service.repository_connection_directory();
            let binding = directory.binding().unwrap();
            directory
                .admit(
                    &binding,
                    RepositoryAuthorityRequest {
                        execution: ExecutionScope {
                            daemon_id: binding.daemon_id.clone(),
                            authority_scope_id: "injected-reader-test".into(),
                            authority_generation: 1,
                        },
                        connection: binding.scope.clone(),
                        target: self.target(RepositoryResourceKind::MergeRequest).repository,
                        use_kind: RepositoryCredentialUse::NativeRead,
                        allowed_transport: RepositoryCredentialTransport::GitlabApi(
                            server.fixture.descriptor.clone(),
                        ),
                    },
                    self.authority.clone(),
                )
                .unwrap()
        }
        fn operation(
            &self,
            server: &ReadServer,
            kind: RepositoryResourceKind,
        ) -> (
            RepositoryReadEligibility,
            RepositoryReadOperation,
            ReviewTarget,
        ) {
            let target = self.target(kind);
            let admission = self.admission(server);
            let eligibility = self
                .auth
                .service
                .gitlab_repository_read_eligibility(&admission)
                .unwrap();
            let operation = RepositoryReadOperation::new(
                self.auth.service.repository_connection_directory(),
                admission,
                self.auth.service.gitlab_repository_secret_reader().unwrap(),
                BUDGET,
                target.clone(),
            )
            .unwrap();
            (eligibility, operation, target)
        }
        fn collection(
            &self,
            server: &ReadServer,
            query: crate::repository_credentials::read::RepositoryCollectionQuery,
        ) -> (
            RepositoryReadEligibility,
            crate::repository_credentials::read::RepositoryCollectionReadOperation,
        ) {
            let admission = self.admission(server);
            let eligibility = self
                .auth
                .service
                .gitlab_repository_read_eligibility(&admission)
                .unwrap();
            let operation =
                crate::repository_credentials::read::RepositoryCollectionReadOperation::new(
                    self.auth.service.repository_connection_directory(),
                    admission,
                    self.auth.service.gitlab_repository_secret_reader().unwrap(),
                    BUDGET,
                    query,
                )
                .unwrap();
            (eligibility, operation)
        }
        async fn refresh(&self, server: &ReadServer) {
            crate::source_control_auth_ops::repository_owner::secret_reader::tests::refresh_original(
                &self.auth.service, &server.fixture.host,
            ).await;
        }
    }

    fn connection(f: &ReadFixture) -> ConnectionObservations {
        let binding = f
            .auth
            .service
            .repository_connection_directory()
            .binding()
            .unwrap();
        ConnectionObservations::new(
            ExecutionScope {
                daemon_id: binding.daemon_id,
                authority_scope_id: "injected-reader-test".into(),
                authority_generation: 1,
            },
            binding.scope,
        )
    }

    // The exact P envelope is consumed by the managed path. No response stamp
    // or denial predicate is constructed by this test bridge.
    async fn issue_read(
        cache: &IssueCache,
        c: &ConnectionObservations,
        t: &ReviewTarget,
        e: &RepositoryReadEligibility,
        f: &ReadFixture,
        age: Duration,
        op: RepositoryReadOperation,
    ) -> std::result::Result<CacheRead<Issue>, CacheFailure> {
        let check = || {
            if f.authority.denied.load(Ordering::SeqCst) {
                Err(intent_core::Error::Internal("fixture retired".into()))
            } else {
                Ok(())
            }
        };
        let fence = |action: &mut CacheAdmissionAction<'_>| {
            if f.authority.denied.load(Ordering::SeqCst) {
                Err(Error::AuthorityDenied)
            } else {
                action()
            }
        };
        let request = ManagedCacheRequest {
            request: CacheRequest {
                connection: c,
                target: t,
                revalidate: &check,
            },
            eligibility: e,
            with_authority: &fence,
        };
        crate::issue_cache::read_managed_issue(cache, &request, age, || op.read_issue()).await
    }
    async fn review_read(
        cache: &PrCache,
        c: &ConnectionObservations,
        t: &ReviewTarget,
        e: &RepositoryReadEligibility,
        f: &ReadFixture,
        age: Duration,
        op: RepositoryReadOperation,
    ) -> std::result::Result<CacheRead<ReviewObservation>, CacheFailure> {
        let check = || {
            if f.authority.denied.load(Ordering::SeqCst) {
                Err(intent_core::Error::Internal("fixture retired".into()))
            } else {
                Ok(())
            }
        };
        let fence = |action: &mut CacheAdmissionAction<'_>| {
            if f.authority.denied.load(Ordering::SeqCst) {
                Err(Error::AuthorityDenied)
            } else {
                action()
            }
        };
        let request = ManagedCacheRequest {
            request: CacheRequest {
                connection: c,
                target: t,
                revalidate: &check,
            },
            eligibility: e,
            with_authority: &fence,
        };
        read_managed_review(
            cache,
            &request,
            PrReadPolicy::Serve { max_age: age },
            &HashSet::new(),
            || async { panic!("Serve never polls") },
            || op.review_observation(),
        )
        .await
    }
    fn provider_error<T: std::fmt::Debug>(
        r: std::result::Result<CacheRead<T>, CacheFailure>,
        kind: ProviderFailureKind,
        status: u16,
    ) {
        let err = r.unwrap_err();
        assert!(
            matches!(&err.cause,CacheError::Provider(intent_sourcecontrol::Error::Provider(f)) if f.kind==kind && f.status==Some(status)),
            "original provider error must survive: {err:?}"
        );
        assert_eq!(err.quota.remaining, Some(23));
    }
    fn serveable<K: Eq + Hash, L, T>(
        cache: &SharedCache<K, L, T>,
        c: &ConnectionObservations,
        t: &ReviewTarget,
    ) -> bool {
        let locked = cache.lock().unwrap();
        let Some(CacheSlot::Qualified(slot)) =
            locked.get(&CacheKey::Qualified(Box::new(c.key(t.clone()))))
        else {
            return false;
        };
        slot.payload
            .as_ref()
            .is_some_and(|p| slot.observations.can_serve(&p.receipt, c))
    }

    #[intent_test_macros::daemon_test]
    async fn managed_uncached_section_preserves_actual_receipt_and_denial() {
        for (status, kind) in [
            (401, ProviderFailureKind::CredentialRejected),
            (403, ProviderFailureKind::ResourceDenied),
            (404, ProviderFailureKind::ResourceDenied),
        ] {
            let s = ReadServer::new().await;
            let f = ReadFixture::new(&s, false).await;
            let c = connection(&f);
            let cache = PrCache::default();
            let (e, op, t) = f.operation(&s, RepositoryResourceKind::MergeRequest);
            review_read(&cache, &c, &t, &e, &f, Duration::ZERO, op)
                .await
                .unwrap();
            let key = CacheKey::Qualified(Box::new(c.key(t.clone())));
            let before = match cache.lock().unwrap().get(&key).unwrap() {
                CacheSlot::Qualified(slot) => slot.payload.as_ref().unwrap().freshness.fetched_at,
                CacheSlot::Legacy(_) => panic!("qualified detail"),
            };
            let check = || Ok(());
            let fence = |action: &mut CacheAdmissionAction<'_>| action();
            let request = ManagedCacheRequest {
                request: CacheRequest {
                    connection: &c,
                    target: &t,
                    revalidate: &check,
                },
                eligibility: &e,
                with_authority: &fence,
            };
            let (_, op, _) = f.operation(&s, RepositoryResourceKind::MergeRequest);
            // The generic uncached consumer uses a real existing typed call.
            // Discussion endpoint wiring remains with the new provider adapter.
            let section = read_managed_review_section(&cache, &request, &HashSet::new(), || {
                op.review_details()
            })
            .await
            .unwrap();
            assert_eq!(section.value.review.number, 4);
            assert_eq!(section.quota.remaining, Some(23));
            section.delivery.with_current(&mut || Ok(())).unwrap();
            if let CacheSlot::Qualified(slot) = cache.lock().unwrap().get(&key).unwrap() {
                assert_eq!(slot.payload.as_ref().unwrap().freshness.fetched_at, before);
            }
            s.status(MR, status);
            let (_, op, _) = f.operation(&s, RepositoryResourceKind::MergeRequest);
            provider_error(
                read_managed_review_section(&cache, &request, &HashSet::new(), || {
                    op.review_details()
                })
                .await,
                kind,
                status,
            );
            assert!(!serveable(&cache, &c, &t));
            assert!(section.delivery.with_current(&mut || Ok(())).is_err());
        }
    }

    async fn collection_read(
        cache: &PrCache,
        c: &ConnectionObservations,
        e: &RepositoryReadEligibility,
        target: &crate::repository_credentials::read::RepositoryCollectionTarget,
        op: crate::repository_credentials::read::RepositoryCollectionReadOperation,
        observer: &CacheDenialObserver<'_>,
    ) -> std::result::Result<summaries::SummaryPage<PullRequest>, CacheFailure> {
        let fence = |action: &mut CacheAdmissionAction<'_>| action();
        let request = summaries::ManagedSummaryRequest {
            request: summaries::SummaryRequest {
                connection: c,
                repository: target.repository(),
                kind: target.kind(),
                revalidate: &|| Ok(()),
            },
            target,
            eligibility: e,
            with_authority: &fence,
            denials: observer,
        };
        summaries::read_managed(
            cache,
            &request,
            || op.reviews(),
            |row| {
                (
                    ReviewTarget {
                        repository: target.repository().clone(),
                        kind: target.kind(),
                        number: row.number,
                    },
                    true,
                )
            },
            |cache| retain_pr_cache(cache, &HashSet::new(), Instant::now()),
        )
        .await
    }

    #[intent_test_macros::daemon_test]
    async fn managed_collection_denial_is_list_only_and_fences_original_page() {
        use crate::repository_credentials::read::RepositoryCollectionQuery as Q;
        let s = ReadServer::new().await;
        let f = ReadFixture::new(&s, false).await;
        let c = connection(&f);
        let sibling = connection(&f);
        let cache = PrCache::default();
        let sibling_cache = PrCache::default();
        let (e, op, t) = f.operation(&s, RepositoryResourceKind::MergeRequest);
        review_read(&cache, &c, &t, &e, &f, Duration::ZERO, op)
            .await
            .unwrap();
        let (e, op) = f.collection(&s, Q::Reviews(intent_sourcecontrol::PrQuery::default()));
        let target = op.target().clone();
        let page = collection_read(&sibling_cache, &sibling, &e, &target, op, &|_| {})
            .await
            .unwrap();
        assert_eq!(page.page.items[0].number, 4);
        let notices = Mutex::new(Vec::new());
        let fanout = |denial: &GuardedCacheDenial<'_>| {
            assert!(denial.apply_to(&sibling));
            notices.lock().unwrap().push(denial.notification());
        };
        s.status("/api/v4/projects/group%2Fproject/merge_requests", 403);
        let (e, op) = f.collection(&s, Q::Reviews(intent_sourcecontrol::PrQuery::default()));
        let target = op.target().clone();
        let failure = collection_read(&cache, &c, &e, &target, op, &fanout)
            .await
            .unwrap_err();
        assert!(
            matches!(failure.cause,CacheError::Provider(intent_sourcecontrol::Error::Provider(ref p))
            if p.kind==ProviderFailureKind::ResourceDenied && p.status==Some(403))
        );
        assert_eq!(failure.quota.remaining, Some(23));
        assert_eq!(
            notices.lock().unwrap()[0].coverage,
            CacheDenialCoverage::List(RepositoryResourceKind::MergeRequest)
        );
        assert!(page.delivery.with_current(&mut || Ok(())).is_err());
        assert!(
            serveable(&cache, &c, &t),
            "list endpoint denial is not item/project denial"
        );
        s.status("/api/v4/projects/group%2Fproject/merge_requests", 200);
        let (e, op) = f.collection(&s, Q::Reviews(intent_sourcecontrol::PrQuery::default()));
        let target = op.target().clone();
        let fresh = collection_read(&sibling_cache, &sibling, &e, &target, op, &|_| {})
            .await
            .unwrap();
        fresh.delivery.with_current(&mut || Ok(())).unwrap();
        assert!(page.delivery.with_current(&mut || Ok(())).is_err());
    }

    #[intent_test_macros::daemon_test]
    async fn managed_collection_wrong_query_and_obsolete_rejection_keep_raw_error_without_fanout() {
        use crate::repository_credentials::read::RepositoryCollectionQuery as Q;
        let s = ReadServer::new().await;
        let f = ReadFixture::new(&s, true).await;
        let c = connection(&f);
        let cache = PrCache::default();
        let (e, op) = f.collection(&s, Q::Reviews(intent_sourcecontrol::PrQuery::default()));
        let (_, other) = f.collection(
            &s,
            Q::Reviews(intent_sourcecontrol::PrQuery {
                search: Some("different".into()),
                ..Default::default()
            }),
        );
        let wrong = other.target().clone();
        let count = AtomicUsize::new(0);
        let observe = |_: &GuardedCacheDenial<'_>| {
            count.fetch_add(1, Ordering::SeqCst);
        };
        assert!(matches!(
            collection_read(&cache, &c, &e, &wrong, op, &observe)
                .await
                .unwrap_err()
                .cause,
            CacheError::Credential(Error::BoundaryMismatch)
        ));
        assert_eq!(count.load(Ordering::SeqCst), 0);
        s.status("/api/v4/projects/group%2Fproject/merge_requests", 401);
        s.pause("/api/v4/projects/group%2Fproject/merge_requests");
        let (e, op) = f.collection(&s, Q::Reviews(intent_sourcecontrol::PrQuery::default()));
        let target = op.target().clone();
        let read = collection_read(&cache, &c, &e, &target, op, &observe);
        let refresh = async {
            s.entered().await;
            f.refresh(&s).await;
            s.resume();
        };
        let (result, ()) = tokio::join!(read, refresh);
        let failure = result.unwrap_err();
        assert!(
            matches!(failure.cause,CacheError::Provider(intent_sourcecontrol::Error::Provider(ref p)) if p.kind==ProviderFailureKind::CredentialRejected)
        );
        assert_eq!(failure.quota.remaining, Some(23));
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[intent_test_macros::daemon_test]
    async fn managed_collection_current_rejection_fans_out_but_quota_failure_does_not() {
        use crate::repository_credentials::read::RepositoryCollectionQuery as Q;
        for status in [401, 429, 503] {
            let s = ReadServer::new().await;
            let f = ReadFixture::new(&s, false).await;
            let c = connection(&f);
            let sibling = connection(&f);
            let cache = PrCache::default();
            let sibling_cache = PrCache::default();
            let t = f.target(RepositoryResourceKind::MergeRequest);
            let (e, op) = f.collection(&s, Q::Reviews(intent_sourcecontrol::PrQuery::default()));
            let target = op.target().clone();
            let old = collection_read(&sibling_cache, &sibling, &e, &target, op, &|_| {})
                .await
                .unwrap();
            let count = AtomicUsize::new(0);
            let observer = |denial: &GuardedCacheDenial<'_>| {
                assert!(denial.apply_to(&sibling));
                count.fetch_add(1, Ordering::SeqCst);
            };
            s.status("/api/v4/projects/group%2Fproject/merge_requests", status);
            let (e, op) = f.collection(&s, Q::Reviews(intent_sourcecontrol::PrQuery::default()));
            let target = op.target().clone();
            let error = collection_read(&cache, &c, &e, &target, op, &observer)
                .await
                .unwrap_err();
            assert!(matches!(error.cause, CacheError::Provider(_)));
            assert_eq!(count.load(Ordering::SeqCst), usize::from(status == 401));
            assert_eq!(
                old.delivery.with_current(&mut || Ok(())).is_ok(),
                status != 401
            );
            if status == 429 {
                assert_eq!(error.quota.remaining, Some(0));
            }
            assert_eq!(t.repository, target.repository().clone());
        }
    }

    #[intent_test_macros::daemon_test]
    async fn observed_issue_denial_uses_actual_evidence_to_fence_sibling() {
        let s = ReadServer::new().await;
        let f = ReadFixture::new(&s, false).await;
        let c = connection(&f);
        let sibling = connection(&f);
        let cache = IssueCache::default();
        let sibling_cache = IssueCache::default();
        let (e, op, t) = f.operation(&s, RepositoryResourceKind::Issue);
        issue_read(&sibling_cache, &sibling, &t, &e, &f, Duration::ZERO, op)
            .await
            .unwrap();
        let count = AtomicUsize::new(0);
        let observer = |denial: &GuardedCacheDenial<'_>| {
            assert!(denial.apply_to(&sibling));
            count.fetch_add(1, Ordering::SeqCst);
        };
        let fence = |action: &mut CacheAdmissionAction<'_>| action();
        let request = ManagedCacheRequest {
            request: CacheRequest {
                connection: &c,
                target: &t,
                revalidate: &|| Ok(()),
            },
            eligibility: &e,
            with_authority: &fence,
        };
        s.status(ISSUE, 404);
        let (_, op, _) = f.operation(&s, RepositoryResourceKind::Issue);
        provider_error(
            crate::issue_cache::read_managed_issue_observed(
                &cache,
                &request,
                &observer,
                Duration::ZERO,
                || op.read_issue(),
            )
            .await,
            ProviderFailureKind::ResourceDenied,
            404,
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert!(!serveable(&sibling_cache, &sibling, &t));
    }

    #[intent_test_macros::daemon_test]
    async fn evicted_observed_response_keeps_error_without_notifying_replacement_groups() {
        let s = ReadServer::new().await;
        let f = ReadFixture::new(&s, false).await;
        let c = connection(&f);
        let sibling = connection(&f);
        let cache = IssueCache::default();
        let sibling_cache = IssueCache::default();
        let (e, op, t) = f.operation(&s, RepositoryResourceKind::Issue);
        issue_read(&cache, &c, &t, &e, &f, Duration::ZERO, op)
            .await
            .unwrap();
        let (_, op, _) = f.operation(&s, RepositoryResourceKind::Issue);
        issue_read(&sibling_cache, &sibling, &t, &e, &f, Duration::ZERO, op)
            .await
            .unwrap();
        let count = AtomicUsize::new(0);
        let observer = |denial: &GuardedCacheDenial<'_>| {
            assert!(denial.apply_to(&sibling));
            count.fetch_add(1, Ordering::SeqCst);
        };
        let fence = |action: &mut CacheAdmissionAction<'_>| action();
        let request = ManagedCacheRequest {
            request: CacheRequest {
                connection: &c,
                target: &t,
                revalidate: &|| Ok(()),
            },
            eligibility: &e,
            with_authority: &fence,
        };
        s.status(ISSUE, 403);
        s.pause(ISSUE);
        let (_, op, _) = f.operation(&s, RepositoryResourceKind::Issue);
        let pending = crate::issue_cache::read_managed_issue_observed(
            &cache,
            &request,
            &observer,
            Duration::ZERO,
            || op.read_issue(),
        );
        let replace = async {
            s.entered().await;
            cache
                .lock()
                .unwrap()
                .remove(&CacheKey::Qualified(Box::new(c.key(t.clone()))));
            s.status(ISSUE, 200);
            let (_, op, _) = f.operation(&s, RepositoryResourceKind::Issue);
            issue_read(&cache, &c, &t, &e, &f, Duration::ZERO, op)
                .await
                .unwrap();
            s.resume();
        };
        let (response, ()) = tokio::join!(pending, replace);
        provider_error(response, ProviderFailureKind::ResourceDenied, 403);
        assert_eq!(
            count.load(Ordering::SeqCst),
            0,
            "an evicted request cannot bypass the original slot fence through its observer"
        );
        assert!(serveable(&cache, &c, &t));
        assert!(serveable(&sibling_cache, &sibling, &t));
    }
    #[intent_test_macros::daemon_test]
    async fn managed_discussion_page_preserves_detail_age_and_final_resource_fence() {
        let s = ReadServer::new().await;
        let f = ReadFixture::new(&s, false).await;
        let c = connection(&f);
        let cache = PrCache::default();
        let (e, op, t) = f.operation(&s, RepositoryResourceKind::MergeRequest);
        let detail = review_read(&cache, &c, &t, &e, &f, Duration::ZERO, op)
            .await
            .unwrap();
        let before = {
            let slots = cache.lock().unwrap();
            let CacheSlot::Qualified(slot) = slots
                .get(&CacheKey::Qualified(Box::new(c.key(t.clone()))))
                .unwrap()
            else {
                panic!()
            };
            slot.payload.as_ref().unwrap().freshness.fetched_at
        };
        let fence = |action: &mut CacheAdmissionAction<'_>| action();
        let request = ManagedCacheRequest {
            request: CacheRequest {
                connection: &c,
                target: &t,
                revalidate: &|| Ok(()),
            },
            eligibility: &e,
            with_authority: &fence,
        };
        let (_, op, _) = f.operation(&s, RepositoryResourceKind::MergeRequest);
        let section = read_managed_review_section(&cache, &request, &HashSet::new(), || {
            op.review_discussions(intent_sourcecontrol::PageParams::first(20))
        })
        .await
        .unwrap();
        assert!(section.value.items.is_empty());
        assert_eq!(section.quota.remaining, Some(23));
        CacheDelivery::with_all_revalidation(
            &[&detail.delivery, &section.delivery],
            &mut |check| check(),
        )
        .unwrap();
        let slots = cache.lock().unwrap();
        let CacheSlot::Qualified(slot) = slots
            .get(&CacheKey::Qualified(Box::new(c.key(t.clone()))))
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(slot.payload.as_ref().unwrap().freshness.fetched_at, before);
        drop(slots);
        c.deny_resource(&t);
        assert!(section.delivery.with_current(&mut || Ok(())).is_err());
    }

    #[intent_test_macros::daemon_test]
    async fn current_401_keeps_raw_error_and_denies_review_and_issue() {
        let s = ReadServer::new().await;
        let f = ReadFixture::new(&s, true).await;
        let c = connection(&f);
        let issues = IssueCache::default();
        let reviews = PrCache::default();
        let (e, op, t) = f.operation(&s, RepositoryResourceKind::Issue);
        issue_read(&issues, &c, &t, &e, &f, Duration::ZERO, op)
            .await
            .unwrap();
        let (re, op, rt) = f.operation(&s, RepositoryResourceKind::MergeRequest);
        let warm = review_read(&reviews, &c, &rt, &re, &f, Duration::ZERO, op)
            .await
            .unwrap();
        assert!(complete_snapshot(&warm.value));
        let (_, op, _) = f.operation(&s, RepositoryResourceKind::Issue);
        s.status(ISSUE, 401);
        provider_error(
            issue_read(&issues, &c, &t, &e, &f, Duration::ZERO, op).await,
            ProviderFailureKind::CredentialRejected,
            401,
        );
        assert!(e.check().is_err());
        assert!(!serveable(&issues, &c, &t));
        assert!(!serveable(&reviews, &c, &rt));
    }

    #[intent_test_macros::daemon_test]
    async fn obsolete_401_does_not_deny_fresh_cache_after_same_binding_refresh() {
        let s = Arc::new(ReadServer::new().await);
        let f = Arc::new(ReadFixture::new(&s, true).await);
        let c = Arc::new(connection(&f));
        let cache = IssueCache::default();
        let (e, op, t) = f.operation(&s, RepositoryResourceKind::Issue);
        issue_read(&cache, &c, &t, &e, &f, Duration::ZERO, op)
            .await
            .unwrap();
        s.status(ISSUE, 401);
        s.pause(ISSUE);
        let pending = {
            let f = f.clone();
            let s = s.clone();
            let c = c.clone();
            let cache = cache.clone();
            tokio::spawn(async move {
                let (e, op, t) = f.operation(&s, RepositoryResourceKind::Issue);
                issue_read(&cache, &c, &t, &e, &f, Duration::ZERO, op).await
            })
        };
        s.entered().await;
        f.refresh(&s).await;
        s.status(ISSUE, 200);
        let (_, op, _) = f.operation(&s, RepositoryResourceKind::Issue);
        issue_read(&cache, &c, &t, &e, &f, Duration::ZERO, op)
            .await
            .unwrap();
        s.resume();
        provider_error(
            pending.await.unwrap(),
            ProviderFailureKind::CredentialRejected,
            401,
        );
        assert!(
            serveable(&cache, &c, &t),
            "old secret rejection must not deny the refreshed cache"
        );
        let before = s.count();
        let (_, op, _) = f.operation(&s, RepositoryResourceKind::Issue);
        assert!(
            !issue_read(&cache, &c, &t, &e, &f, Duration::from_secs(60), op)
                .await
                .unwrap()
                .fetched
        );
        assert_eq!(s.count(), before);
    }

    #[intent_test_macros::daemon_test]
    async fn obsolete_project_denial_does_not_deny_a_refreshed_review() {
        let s = Arc::new(ReadServer::new().await);
        let f = Arc::new(ReadFixture::new(&s, true).await);
        let c = Arc::new(connection(&f));
        let cache = PrCache::default();
        let (e, op, t) = f.operation(&s, RepositoryResourceKind::MergeRequest);
        assert!(complete_snapshot(
            &review_read(&cache, &c, &t, &e, &f, Duration::ZERO, op)
                .await
                .unwrap()
                .value
        ));
        s.status(PROJECT, 403);
        s.pause(PROJECT);
        let pending = {
            let f = f.clone();
            let s = s.clone();
            let c = c.clone();
            let cache = cache.clone();
            tokio::spawn(async move {
                let (e, op, t) = f.operation(&s, RepositoryResourceKind::MergeRequest);
                review_read(&cache, &c, &t, &e, &f, Duration::ZERO, op).await
            })
        };
        s.entered().await;
        f.refresh(&s).await;
        s.status(PROJECT, 200);
        let (_, op, _) = f.operation(&s, RepositoryResourceKind::MergeRequest);
        review_read(&cache, &c, &t, &e, &f, Duration::ZERO, op)
            .await
            .unwrap();
        s.resume();
        provider_error(
            pending.await.unwrap(),
            ProviderFailureKind::ProjectDenied,
            403,
        );
        assert!(
            serveable(&cache, &c, &t),
            "obsolete project response cannot deny newer settled source"
        );
    }
    #[intent_test_macros::daemon_test]
    async fn same_revision_late_project_denial_is_not_waived_by_newer_success() {
        let s = Arc::new(ReadServer::new().await);
        let f = Arc::new(ReadFixture::new(&s, true).await);
        let c = Arc::new(connection(&f));
        let reviews = PrCache::default();
        let issues = IssueCache::default();
        let (e, op, t) = f.operation(&s, RepositoryResourceKind::MergeRequest);
        review_read(&reviews, &c, &t, &e, &f, Duration::ZERO, op)
            .await
            .unwrap();
        let (ie, op, it) = f.operation(&s, RepositoryResourceKind::Issue);
        issue_read(&issues, &c, &it, &ie, &f, Duration::ZERO, op)
            .await
            .unwrap();
        s.status(PROJECT, 404);
        s.pause(PROJECT);
        let pending = {
            let f = f.clone();
            let s = s.clone();
            let c = c.clone();
            let reviews = reviews.clone();
            tokio::spawn(async move {
                let (e, op, t) = f.operation(&s, RepositoryResourceKind::MergeRequest);
                review_read(&reviews, &c, &t, &e, &f, Duration::ZERO, op).await
            })
        };
        s.entered().await;
        s.status(PROJECT, 200);
        let (_, op, _) = f.operation(&s, RepositoryResourceKind::MergeRequest);
        review_read(&reviews, &c, &t, &e, &f, Duration::ZERO, op)
            .await
            .unwrap();
        s.resume();
        provider_error(
            pending.await.unwrap(),
            ProviderFailureKind::ProjectDenied,
            404,
        );
        assert!(!serveable(&reviews, &c, &t));
        assert!(!serveable(&issues, &c, &it));
        let (_, op, _) = f.operation(&s, RepositoryResourceKind::Issue);
        issue_read(&issues, &c, &it, &ie, &f, Duration::ZERO, op)
            .await
            .unwrap();
        assert!(serveable(&issues, &c, &it));
        assert!(
            !serveable(&reviews, &c, &t),
            "fresh issue cannot revive old review"
        );
    }

    #[intent_test_macros::daemon_test]
    async fn quota_partial_keeps_old_cache_time_and_hit_but_blocks_dispatch() {
        let s = ReadServer::new().await;
        let f = ReadFixture::new(&s, false).await;
        let c = connection(&f);
        let cache = PrCache::default();
        let (e, op, t) = f.operation(&s, RepositoryResourceKind::MergeRequest);
        review_read(&cache, &c, &t, &e, &f, Duration::ZERO, op)
            .await
            .unwrap();
        let key = CacheKey::Qualified(Box::new(c.key(t.clone())));
        let before = {
            let locked = cache.lock().unwrap();
            let CacheSlot::Qualified(slot) = locked.get(&key).unwrap() else {
                panic!()
            };
            slot.payload.as_ref().unwrap().freshness
        };
        s.status(PROJECT, 429);
        let calls = s.count();
        let (_, op, _) = f.operation(&s, RepositoryResourceKind::MergeRequest);
        let partial = review_read(&cache, &c, &t, &e, &f, Duration::ZERO, op)
            .await
            .unwrap();
        assert_eq!(partial.quota.remaining, Some(0));
        assert_eq!(
            partial.value.availability.policy,
            intent_sourcecontrol::ProviderAvailability::RateLimited
        );
        assert!(!complete_snapshot(&partial.value));
        assert_eq!(s.count(), calls + 2);
        e.check().unwrap();
        {
            let locked = cache.lock().unwrap();
            let CacheSlot::Qualified(slot) = locked.get(&key).unwrap() else {
                panic!()
            };
            let current = slot.payload.as_ref().unwrap().freshness;
            assert_eq!(current.fetched_at, before.fetched_at);
            assert_eq!(current.refreshed_at, before.refreshed_at);
            assert!(!slot.reuse_allowed);
        }
        let (_, op, _) = f.operation(&s, RepositoryResourceKind::MergeRequest);
        let hit = review_read(&cache, &c, &t, &e, &f, Duration::from_secs(60), op)
            .await
            .unwrap();
        assert!(!hit.fetched);
        assert!(complete_snapshot(&hit.value));
        assert_eq!(s.count(), calls + 2);
        let (_, op, _) = f.operation(&s, RepositoryResourceKind::MergeRequest);
        let failed = review_read(&cache, &c, &t, &e, &f, Duration::ZERO, op)
            .await
            .unwrap_err();
        assert!(matches!(
            failed.cause,
            CacheError::Provider(intent_sourcecontrol::Error::AdmissionUnavailable(
                intent_sourcecontrol::error::AdmissionUnavailable::Backoff
            ))
        ));
        assert_eq!(s.count(), calls + 2);
    }

    #[intent_test_macros::daemon_test]
    async fn caller_refusal_keeps_observed_error_and_quota_without_applying_it() {
        let s = Arc::new(ReadServer::new().await);
        let f = Arc::new(ReadFixture::new(&s, false).await);
        let c = Arc::new(connection(&f));
        let cache = IssueCache::default();
        let (e, op, t) = f.operation(&s, RepositoryResourceKind::Issue);
        issue_read(&cache, &c, &t, &e, &f, Duration::ZERO, op)
            .await
            .unwrap();
        s.status(ISSUE, 403);
        s.pause(ISSUE);
        let pending = {
            let f = f.clone();
            let s = s.clone();
            let c = c.clone();
            let cache = cache.clone();
            tokio::spawn(async move {
                let (e, op, t) = f.operation(&s, RepositoryResourceKind::Issue);
                issue_read(&cache, &c, &t, &e, &f, Duration::ZERO, op).await
            })
        };
        s.entered().await;
        f.authority.denied.store(true, Ordering::SeqCst);
        s.resume();
        provider_error(
            pending.await.unwrap(),
            ProviderFailureKind::ResourceDenied,
            403,
        );
        assert!(
            serveable(&cache, &c, &t),
            "refused application does not mutate the slot"
        );
        let (_, op, _) = f.operation(&s, RepositoryResourceKind::Issue);
        let calls = s.count();
        assert!(
            issue_read(&cache, &c, &t, &e, &f, Duration::from_secs(60), op)
                .await
                .is_err()
        );
        assert_eq!(s.count(), calls);
    }

    #[intent_test_macros::daemon_test]
    async fn foreign_success_and_denial_receipts_cannot_apply_to_the_requested_item() {
        for status in [200, 404] {
            let s = ReadServer::new().await;
            let f = ReadFixture::new(&s, false).await;
            let c = connection(&f);
            let cache = IssueCache::default();
            let (e, op, t) = f.operation(&s, RepositoryResourceKind::Issue);
            issue_read(&cache, &c, &t, &e, &f, Duration::ZERO, op)
                .await
                .unwrap();
            let mut other = t.clone();
            other.number += 1;
            let op = RepositoryReadOperation::new(
                f.auth.service.repository_connection_directory(),
                f.admission(&s),
                f.auth.service.gitlab_repository_secret_reader().unwrap(),
                BUDGET,
                other,
            )
            .unwrap();
            s.status("/api/v4/projects/group%2Fproject/issues/5", status);
            let result = issue_read(&cache, &c, &t, &e, &f, Duration::ZERO, op).await;
            if status == 200 {
                assert!(matches!(
                    result.unwrap_err().cause,
                    CacheError::Credential(Error::BoundaryMismatch)
                ));
            } else {
                provider_error(result, ProviderFailureKind::ResourceDenied, 404);
            }
            assert!(serveable(&cache, &c, &t));
        }
    }

    #[intent_test_macros::daemon_test]
    async fn observed_source_change_blocks_hits_even_after_old_bytes_are_restored() {
        let s = ReadServer::new().await;
        let f = ReadFixture::new(&s, false).await;
        let c = connection(&f);
        let cache = IssueCache::default();
        let (e, op, t) = f.operation(&s, RepositoryResourceKind::Issue);
        issue_read(&cache, &c, &t, &e, &f, Duration::ZERO, op)
            .await
            .unwrap();
        let (_, op, _) = f.operation(&s, RepositoryResourceKind::Issue);
        let calls = s.count();
        f.auth
            .service
            .gitlab_secret_store
            .store(
                intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT,
                "different-disposable-bytes",
            )
            .unwrap();
        let err = issue_read(&cache, &c, &t, &e, &f, Duration::ZERO, op)
            .await
            .unwrap_err();
        assert!(matches!(
            err.cause,
            CacheError::Provider(intent_sourcecontrol::Error::AdmissionUnavailable(_))
        ));
        assert_eq!(s.count(), calls);
        f.auth
            .service
            .gitlab_secret_store
            .store(
                intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT,
                "stored-pat",
            )
            .unwrap();
        assert!(e.check().is_err());
        let (_, op, _) = f.operation(&s, RepositoryResourceKind::Issue);
        assert!(
            issue_read(&cache, &c, &t, &e, &f, Duration::from_secs(60), op)
                .await
                .is_err()
        );
        assert_eq!(s.count(), calls);
    }

    #[intent_test_macros::daemon_test]
    async fn poll_full_failure_uses_its_own_response_instead_of_primary_receipt() {
        let s = ReadServer::new().await;
        let f = ReadFixture::new(&s, false).await;
        let c = connection(&f);
        let cache = PrCache::default();
        let (e, op, t) = f.operation(&s, RepositoryResourceKind::MergeRequest);
        review_read(&cache, &c, &t, &e, &f, Duration::ZERO, op)
            .await
            .unwrap();
        // Force a full optional-field attempt after the next primary response.
        {
            let mut locked = cache.lock().unwrap();
            let CacheSlot::Qualified(slot) = locked
                .get_mut(&CacheKey::Qualified(Box::new(c.key(t.clone()))))
                .unwrap()
            else {
                panic!()
            };
            slot.reuse_allowed = false;
        }
        let (_, primary, _) = f.operation(&s, RepositoryResourceKind::MergeRequest);
        let (_, full, _) = f.operation(&s, RepositoryResourceKind::MergeRequest);
        let check = || Ok(());
        let fence = |action: &mut CacheAdmissionAction<'_>| action();
        let request = ManagedCacheRequest {
            request: CacheRequest {
                connection: &c,
                target: &t,
                revalidate: &check,
            },
            eligibility: &e,
            with_authority: &fence,
        };
        let result = read_managed_review(
            &cache,
            &request,
            PrReadPolicy::Poll,
            &HashSet::new(),
            || primary.review_details(),
            || async {
                s.status(PROJECT, 403);
                full.review_observation().await
            },
        )
        .await;
        provider_error(result, ProviderFailureKind::ProjectDenied, 403);
        assert!(!serveable(&cache, &c, &t));
    }
    #[intent_test_macros::daemon_test]
    async fn managed_pre_denial_success_cannot_revive_after_fresh_recovery() {
        let s = Arc::new(ReadServer::new().await);
        let f = Arc::new(ReadFixture::new(&s, false).await);
        let c = Arc::new(connection(&f));
        let cache = IssueCache::default();
        let (e, op, t) = f.operation(&s, RepositoryResourceKind::Issue);
        issue_read(&cache, &c, &t, &e, &f, Duration::ZERO, op)
            .await
            .unwrap();
        s.pause(ISSUE);
        let pending = {
            let f = f.clone();
            let s = s.clone();
            let c = c.clone();
            let cache = cache.clone();
            tokio::spawn(async move {
                let (e, op, t) = f.operation(&s, RepositoryResourceKind::Issue);
                issue_read(&cache, &c, &t, &e, &f, Duration::ZERO, op).await
            })
        };
        s.entered().await;
        s.status(ISSUE, 404);
        let (_, op, _) = f.operation(&s, RepositoryResourceKind::Issue);
        provider_error(
            issue_read(&cache, &c, &t, &e, &f, Duration::ZERO, op).await,
            ProviderFailureKind::ResourceDenied,
            404,
        );
        s.status(ISSUE, 200);
        let (_, op, _) = f.operation(&s, RepositoryResourceKind::Issue);
        issue_read(&cache, &c, &t, &e, &f, Duration::ZERO, op)
            .await
            .unwrap();
        s.resume();
        let old = pending.await.unwrap().unwrap_err();
        assert!(matches!(old.cause, CacheError::Ineligible(_)));
        assert_eq!(old.quota.remaining, Some(23));
        assert!(serveable(&cache, &c, &t));
    }

    #[intent_test_macros::daemon_test]
    async fn managed_eviction_fences_late_success_and_keeps_late_error_unchanged() {
        for status in [200, 403] {
            let s = Arc::new(ReadServer::new().await);
            let f = Arc::new(ReadFixture::new(&s, false).await);
            let c = Arc::new(connection(&f));
            let cache = IssueCache::default();
            let (e, op, t) = f.operation(&s, RepositoryResourceKind::Issue);
            issue_read(&cache, &c, &t, &e, &f, Duration::ZERO, op)
                .await
                .unwrap();
            s.status(ISSUE, status);
            s.pause(ISSUE);
            let pending = {
                let f = f.clone();
                let s = s.clone();
                let c = c.clone();
                let cache = cache.clone();
                tokio::spawn(async move {
                    let (e, op, t) = f.operation(&s, RepositoryResourceKind::Issue);
                    issue_read(&cache, &c, &t, &e, &f, Duration::ZERO, op).await
                })
            };
            s.entered().await;
            cache
                .lock()
                .unwrap()
                .remove(&CacheKey::Qualified(Box::new(c.key(t.clone()))));
            s.status(ISSUE, 200);
            let (_, op, _) = f.operation(&s, RepositoryResourceKind::Issue);
            issue_read(&cache, &c, &t, &e, &f, Duration::ZERO, op)
                .await
                .unwrap();
            s.resume();
            let old = pending.await.unwrap();
            if status == 200 {
                assert!(matches!(
                    old.unwrap_err().cause,
                    CacheError::Ineligible(Ineligible::DifferentSlot)
                ));
            } else {
                provider_error(old, ProviderFailureKind::ResourceDenied, 403);
            }
            assert!(serveable(&cache, &c, &t));
        }
    }

    #[intent_test_macros::daemon_test]
    async fn obsolete_item_denial_does_not_apply_after_refresh() {
        for status in [403, 404] {
            let s = Arc::new(ReadServer::new().await);
            let f = Arc::new(ReadFixture::new(&s, true).await);
            let c = Arc::new(connection(&f));
            let cache = IssueCache::default();
            let (e, op, t) = f.operation(&s, RepositoryResourceKind::Issue);
            issue_read(&cache, &c, &t, &e, &f, Duration::ZERO, op)
                .await
                .unwrap();
            s.status(ISSUE, status);
            s.pause(ISSUE);
            let pending = {
                let f = f.clone();
                let s = s.clone();
                let c = c.clone();
                let cache = cache.clone();
                tokio::spawn(async move {
                    let (e, op, t) = f.operation(&s, RepositoryResourceKind::Issue);
                    issue_read(&cache, &c, &t, &e, &f, Duration::ZERO, op).await
                })
            };
            s.entered().await;
            f.refresh(&s).await;
            s.status(ISSUE, 200);
            let (_, op, _) = f.operation(&s, RepositoryResourceKind::Issue);
            issue_read(&cache, &c, &t, &e, &f, Duration::ZERO, op)
                .await
                .unwrap();
            s.resume();
            provider_error(
                pending.await.unwrap(),
                ProviderFailureKind::ResourceDenied,
                status,
            );
            assert!(serveable(&cache, &c, &t));
        }
    }

    #[intent_test_macros::daemon_test]
    async fn managed_success_requires_its_opaque_response_receipt() {
        let s = ReadServer::new().await;
        let f = ReadFixture::new(&s, false).await;
        let c = connection(&f);
        let cache = IssueCache::default();
        let (e, _op, t) = f.operation(&s, RepositoryResourceKind::Issue);
        let check = || Ok(());
        let fence = |action: &mut CacheAdmissionAction<'_>| action();
        let request = ManagedCacheRequest {
            request: CacheRequest {
                connection: &c,
                target: &t,
                revalidate: &check,
            },
            eligibility: &e,
            with_authority: &fence,
        };
        let access = DetailAccess::Managed(&request);
        let Started::Read { ticket, .. } =
            start_access(&cache, access, Duration::ZERO, |_| {}).unwrap()
        else {
            panic!()
        };
        // A legacy tuple is not proof for the managed success branch.
        let result = finish_access(
            &cache,
            access,
            ticket,
            (Ok(super::issue(4, "unattributed")), quota()).into(),
            |_| true,
            Freshness::full(),
            |_| {},
        );
        assert!(matches!(
            result.unwrap_err().cause,
            CacheError::Credential(Error::BoundaryMismatch)
        ));
        assert!(!serveable(&cache, &c, &t));
        assert_eq!(s.count(), 0);
    }
}

#[tokio::test]
async fn retained_cache_delivery_rechecks_original_complete_and_partial_observations() {
    for partial in [false, true] {
        let cache = PrCache::default();
        let conn = connection("delivery");
        let target = mr("team/app", 7);
        let req = request(&conn, &target);
        let mut value = observation(7, "original");
        if partial {
            value.availability.approvals = ProviderAvailability::Restricted;
        }
        let returned = review(&cache, &req, Duration::ZERO, Ok(value))
            .await
            .unwrap();
        let mut sent = 0;
        returned
            .delivery
            .with_current(&mut || {
                sent += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(sent, 1);
        assert!(review(
            &cache,
            &req,
            Duration::ZERO,
            Err(denied(ProviderFailureKind::ResourceDenied))
        )
        .await
        .is_err());
        assert!(returned
            .delivery
            .with_current(&mut || {
                sent += 1;
                Ok(())
            })
            .is_err());
        assert_eq!(sent, 1);
        let fresh = review(&cache, &req, Duration::ZERO, Ok(observation(7, "fresh")))
            .await
            .unwrap();
        fresh
            .delivery
            .with_current(&mut || {
                sent += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(sent, 2);
        assert!(returned
            .delivery
            .with_current(&mut || {
                sent += 1;
                Ok(())
            })
            .is_err());
        assert_eq!(sent, 2);
        cache.lock().unwrap().clear();
        assert!(fresh
            .delivery
            .with_current(&mut || {
                sent += 1;
                Ok(())
            })
            .is_err());
        assert_eq!(sent, 2);
    }
}

#[tokio::test]
async fn cache_delivery_batch_locks_shared_and_distinct_maps_once() {
    let reviews = PrCache::default();
    let issues = IssueCache::default();
    let conn = connection("batch-delivery");
    let a = mr("team/app", 7);
    let b = mr("team/app", 8);
    let i = target("team/app", RepositoryResourceKind::Issue, 9);
    let first = review(
        &reviews,
        &request(&conn, &a),
        Duration::ZERO,
        Ok(observation(7, "first")),
    )
    .await
    .unwrap();
    let second = review(
        &reviews,
        &request(&conn, &b),
        Duration::ZERO,
        Ok(observation(8, "second")),
    )
    .await
    .unwrap();
    let third = issue_read(
        &issues,
        &request(&conn, &i),
        Duration::ZERO,
        Ok(issue(9, "third")),
    )
    .await
    .unwrap();
    let aliases = [
        &third.delivery,
        &first.delivery,
        &second.delivery,
        &first.delivery,
    ];
    let mut calls = 0;
    let original_error = CacheDelivery::with_all_current(&aliases, &mut || {
        assert!(reviews.try_lock().is_err());
        assert!(issues.try_lock().is_err());
        calls += 1;
        Err(RepositoryCredentialError::AuthorityDenied)
    });
    assert!(matches!(
        original_error,
        Err(RepositoryCredentialError::AuthorityDenied)
    ));
    assert_eq!(calls, 1);
    assert!(reviews.try_lock().is_ok());
    assert!(issues.try_lock().is_ok());
    assert!(issue_read(
        &issues,
        &request(&conn, &i),
        Duration::ZERO,
        Err(denied(ProviderFailureKind::ProjectDenied))
    )
    .await
    .is_err());
    assert!(CacheDelivery::with_all_current(&aliases, &mut || {
        calls += 1;
        Ok(())
    })
    .is_err());
    assert_eq!(
        calls, 1,
        "no consumer action after any original observation was denied"
    );
}

#[tokio::test]
async fn uncached_section_preserves_detail_freshness_and_cannot_recover_old_detail() {
    let cache = PrCache::default();
    let conn = connection("sections");
    let target = mr("team/app", 7);
    let request = request(&conn, &target);
    warm(&cache, &request, "retained detail").await;
    let before = match cache.lock().unwrap().get(&request.key()).unwrap() {
        CacheSlot::Qualified(slot) => slot.payload.as_ref().unwrap().freshness.fetched_at,
        CacheSlot::Legacy(_) => panic!("qualified detail"),
    };
    let section = read_review_section_access(
        &cache,
        DetailAccess::Legacy(&request),
        &HashSet::new(),
        || async { (Ok(vec!["general note", "diff reply"]), quota()).into() },
    )
    .await
    .unwrap();
    assert_eq!(section.value, ["general note", "diff reply"]);
    assert_eq!(section.quota, quota());
    section.delivery.with_current(&mut || Ok(())).unwrap();
    if let CacheSlot::Qualified(slot) = cache.lock().unwrap().get(&request.key()).unwrap() {
        assert_eq!(slot.payload.as_ref().unwrap().freshness.fetched_at, before);
        assert!(slot.reuse_allowed);
    }
    assert!(review(
        &cache,
        &request,
        Duration::ZERO,
        Err(denied(ProviderFailureKind::ResourceDenied))
    )
    .await
    .is_err());
    assert!(section.delivery.with_current(&mut || Ok(())).is_err());
    let fresh_section = read_review_section_access(
        &cache,
        DetailAccess::Legacy(&request),
        &HashSet::new(),
        || async { (Ok("fresh note"), quota()).into() },
    )
    .await
    .unwrap();
    fresh_section.delivery.with_current(&mut || Ok(())).unwrap();
    let locked = cache.lock().unwrap();
    let CacheSlot::Qualified(slot) = locked.get(&request.key()).unwrap() else {
        panic!("qualified detail")
    };
    let retained = slot.payload.as_ref().unwrap();
    assert_eq!(retained.freshness.fetched_at, before);
    assert!(!slot.observations.can_serve(&retained.receipt, &conn));
}

#[tokio::test]
async fn uncached_section_pending_result_cannot_survive_denial_and_fresh_recovery() {
    let cache = PrCache::default();
    let conn = connection("sections");
    let target = mr("team/app", 7);
    let request = request(&conn, &target);
    warm(&cache, &request, "old detail").await;
    let (arrived, arrival) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let monitored = HashSet::new();
    let pending = read_review_section_access(
        &cache,
        DetailAccess::Legacy(&request),
        &monitored,
        || async {
            arrived.send(()).unwrap();
            released.await.unwrap();
            (Ok("old note"), quota()).into()
        },
    );
    let change = async {
        arrival.await.unwrap();
        assert!(review(
            &cache,
            &request,
            Duration::ZERO,
            Err(denied(ProviderFailureKind::ProjectDenied))
        )
        .await
        .is_err());
        warm(&cache, &request, "fresh detail").await;
        release.send(()).unwrap();
    };
    let (old, ()) = tokio::join!(pending, change);
    let failure = old.unwrap_err();
    assert!(
        matches!(
            failure.cause,
            CacheError::Ineligible(Ineligible::DeniedSinceRequest | Ineligible::OlderObservation)
        ),
        "{failure:?}"
    );
    assert_eq!(failure.quota, quota());
    assert_eq!(
        hit(&cache, &request)
            .await
            .unwrap()
            .value
            .details
            .review
            .title,
        "fresh detail"
    );
}

#[tokio::test]
async fn uncached_section_failure_keeps_detail_readable_and_returns_original_quota() {
    let cache = PrCache::default();
    let conn = connection("sections");
    let target = mr("team/app", 7);
    let request = request(&conn, &target);
    warm(&cache, &request, "retained detail").await;
    for error in [
        denied(ProviderFailureKind::OptionalUnavailable),
        intent_sourcecontrol::Error::RateLimited("Retry-After: 60".into()),
    ] {
        let expected = error.to_string();
        let result: Result<CacheRead<String>, _> = read_review_section_access(
            &cache,
            DetailAccess::Legacy(&request),
            &HashSet::new(),
            || async { (Err(error), quota()).into() },
        )
        .await;
        let failure = result.unwrap_err();
        assert_eq!(failure.quota, quota());
        let CacheError::Provider(error) = failure.cause else {
            panic!("original error")
        };
        assert_eq!(error.to_string(), expected);
        assert!(!hit(&cache, &request).await.unwrap().fetched);
        if let CacheSlot::Qualified(slot) = cache.lock().unwrap().get(&request.key()).unwrap() {
            assert!(
                slot.reuse_allowed,
                "section failures do not invalidate complete detail fields"
            );
        }
    }
}

#[tokio::test]
async fn shared_resource_denial_fences_independent_maps_without_denial_spread() {
    let first = PrCache::default();
    let second = PrCache::default();
    let conn = connection("same admitted connection");
    let target = mr("team/app", 7);
    let other = mr("team/app", 8);
    let request = request(&conn, &target);
    let other_request = super::tests::request(&conn, &other);
    warm(&second, &request, "old detail").await;
    let old = hit(&second, &request).await.unwrap();
    warm(&first, &request, "first detail").await;
    warm(&second, &other_request, "unrelated detail").await;
    let unrelated = hit(&second, &other_request).await.unwrap();
    assert!(review(
        &first,
        &request,
        Duration::ZERO,
        Err(denied(ProviderFailureKind::ResourceDenied))
    )
    .await
    .is_err());
    assert!(
        old.delivery.with_current(&mut || Ok(())).is_err(),
        "a resource denial in another map sharing the original lifetime must retire this delivery"
    );
    unrelated.delivery.with_current(&mut || Ok(())).unwrap();
    warm(&second, &request, "fresh detail").await;
    let fresh = hit(&second, &request).await.unwrap();
    fresh.delivery.with_current(&mut || Ok(())).unwrap();
    assert!(old.delivery.with_current(&mut || Ok(())).is_err());
}

#[tokio::test]
async fn final_revalidation_refuses_metadata_denial_after_initial_map_check() {
    let cache = PrCache::default();
    let conn = connection("original");
    let target = mr("team/app", 7);
    let req = request(&conn, &target);
    warm(&cache, &req, "private").await;
    let output = hit(&cache, &req).await.unwrap();
    let transferred = AtomicBool::new(false);
    let result = output.delivery.with_revalidation(&mut |check| {
        check()?;
        conn.deny_resource(&target);
        check()?;
        transferred.store(true, Ordering::SeqCst);
        Ok(())
    });
    assert!(matches!(result, Err(RepositoryCredentialError::Retired)));
    assert!(!transferred.load(Ordering::SeqCst));
    warm(&cache, &req, "fresh").await;
    let fresh = hit(&cache, &req).await.unwrap();
    CacheDelivery::with_all_revalidation(&[&fresh.delivery, &fresh.delivery], &mut |check| {
        check()?;
        conn.deny_project(&target.repository);
        check()
    })
    .unwrap_err();
}

#[tokio::test]
async fn guarded_denial_breadth_isolates_context_account_instance_kind_and_coverage() {
    for coverage in [
        CacheDenialCoverage::Resource(mr("team/app", 7)),
        CacheDenialCoverage::Project,
        CacheDenialCoverage::Connection,
        CacheDenialCoverage::List(RepositoryResourceKind::MergeRequest),
    ] {
        let original = connection("same");
        let recipient = connection("same");
        let other_account = connection("other");
        let cache = PrCache::default();
        let issue_cache = IssueCache::default();
        let t = mr("team/app", 7);
        let other = mr("team/other", 7);
        let mut instance = t.clone();
        instance.repository.instance_base_url.push_str("/other");
        let issue_t = target("team/app", RepositoryResourceKind::Issue, 7);
        for target in [&t, &other, &instance] {
            warm(&cache, &request(&recipient, target), "retained").await;
        }
        warm(&cache, &request(&other_account, &t), "different account").await;
        issue_read(
            &issue_cache,
            &request(&recipient, &issue_t),
            Duration::ZERO,
            Ok(issue(7, "issue")),
        )
        .await
        .unwrap();
        let notification = AcceptedCacheDenial {
            repository: t.repository.clone(),
            coverage: coverage.clone(),
        };
        let guarded = GuardedCacheDenial {
            original: &original,
            accepted: &notification,
        };
        assert!(guarded.apply_to(&recipient));
        assert!(!guarded.apply_to(&other_account));
        let can_serve = |target: &ReviewTarget| {
            let map = cache.lock().unwrap();
            let CacheSlot::Qualified(slot) = map.get(&request(&recipient, target).key()).unwrap()
            else {
                panic!()
            };
            slot.observations
                .can_serve(&slot.payload.as_ref().unwrap().receipt, &recipient)
        };
        assert_eq!(
            can_serve(&t),
            matches!(coverage, CacheDenialCoverage::List(_))
        );
        assert_eq!(
            can_serve(&other),
            !matches!(coverage, CacheDenialCoverage::Connection)
        );
        assert_eq!(
            can_serve(&instance),
            !matches!(coverage, CacheDenialCoverage::Connection)
        );
        let issue_current = {
            let map = issue_cache.lock().unwrap();
            let CacheSlot::Qualified(slot) = map.get(&request(&recipient, &issue_t).key()).unwrap()
            else {
                panic!()
            };
            slot.observations
                .can_serve(&slot.payload.as_ref().unwrap().receipt, &recipient)
        };
        assert_eq!(
            issue_current,
            matches!(
                coverage,
                CacheDenialCoverage::Resource(_) | CacheDenialCoverage::List(_)
            )
        );
        hit(&cache, &request(&other_account, &t)).await.unwrap();
        recipient.retire();
        assert!(!guarded.apply_to(&recipient));
    }
}
