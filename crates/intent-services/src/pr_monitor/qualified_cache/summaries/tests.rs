use std::sync::atomic::{AtomicBool, Ordering};

use intent_core::{ExecutionScope, RepositoryConnectionScope, RepositoryProvider};
use intent_sourcecontrol::{
    error::{ProviderFailure, ProviderFailureKind},
    Error, Issue, MergeRequirementSignals, ProviderAvailability, PullRequest, ReviewAvailability,
    ReviewDetails, ReviewObservation, ReviewThreadTally,
};
use serde_json::json;
use tokio::sync::oneshot;

use super::super::CacheRead;
use super::*;
use crate::issue_cache::{read_qualified_issue, IssueCache};
use crate::observation_adapter::complete_snapshot;
use crate::pr_monitor::{
    LegacyPrCacheSlot, PrCache, PrReadPolicy, NONE, PR_CACHE_MAX_ENTRIES, PR_CACHE_MAX_IDLE,
};

const FRESH: Duration = Duration::from_secs(60);

#[test]
fn an_evicted_receipt_does_not_retain_history_receipt_lifetime_regression() {
    let connection = connection("alice");
    let repo = repository("team/app");
    let request = req(&connection, &repo, RepositoryResourceKind::MergeRequest);
    let cache = PrCache::default();
    let result = reviews(&cache, &request, &begin(&request).unwrap(), &[7]);
    cache.lock().unwrap().clear();
    assert!(!can_serve(
        &cache,
        &request,
        result.receipts[0].as_ref().unwrap(),
        FRESH
    )
    .unwrap());
    assert_eq!(
        connection.retained_project_count(),
        0,
        "copied/retained expired receipts must not pin project history"
    );
}

#[test]
fn an_empty_denied_page_is_not_fresh_receipt_lifetime_regression() {
    let connection = connection("alice");
    let repo = repository("team/app");
    let request = req(&connection, &repo, RepositoryResourceKind::MergeRequest);
    let cache = PrCache::default();
    let old = begin(&request).unwrap();
    let _error = failure(
        &request,
        &old,
        denied(ProviderFailureKind::ProjectDenied),
        quota(),
    );
    let late = finish(
        &cache,
        &request,
        &old,
        page(Vec::<PullRequest>::new()),
        |p| (target(&request, p.number), true),
        keep,
    )
    .unwrap_err();
    assert!(matches!(
        late.cause,
        CacheError::Ineligible(Ineligible::DeniedSinceRequest)
    ));
    assert_eq!(late.quota, quota());
}

fn connection(account: &str) -> ConnectionObservations {
    ConnectionObservations::new(
        ExecutionScope {
            daemon_id: "daemon".into(),
            authority_scope_id: "owner".into(),
            authority_generation: 9_007_199_254_740_995,
        },
        RepositoryConnectionScope {
            connection_id: "connection".into(),
            account_id: account.into(),
            connection_generation: u64::MAX,
        },
    )
}
fn repository(path: &str) -> RepositoryTarget {
    RepositoryTarget {
        provider: RepositoryProvider::Gitlab,
        instance_base_url: "https://forge.example:8443/gitlab".into(),
        project_path: path.into(),
    }
}
fn req<'a>(
    connection: &'a ConnectionObservations,
    repository: &'a RepositoryTarget,
    kind: RepositoryResourceKind,
) -> SummaryRequest<'a> {
    SummaryRequest {
        connection,
        repository,
        kind,
        revalidate: &|| Ok(()),
    }
}
fn target(request: &SummaryRequest<'_>, number: u64) -> ReviewTarget {
    ReviewTarget {
        repository: request.repository.clone(),
        kind: request.kind,
        number,
    }
}
fn quota() -> RateLimitStatus {
    RateLimitStatus {
        remaining: Some(0),
        reset_at: Some(2_000_000_099),
        limit: Some(100),
    }
}
fn denied(kind: ProviderFailureKind) -> Error {
    Error::Provider(ProviderFailure {
        kind,
        status: Some(403),
    })
}
fn pull(number: u64) -> PullRequest {
    serde_json::from_value(json!({"number":number,"url":"https://forge.example/item", "title":format!("row {number}"),"state":"open","draft":false,"sourceBranch":"feature","targetBranch":"main","author":"alice","createdAt":"","updatedAt":"2026-09-27T15:00:00Z"})).unwrap()
}
fn detail(number: u64) -> ReviewObservation {
    ReviewObservation {
        details: ReviewDetails {
            review: pull(number),
            source: None,
            target: None,
            confirmed_draft: None,
            confirmed_state: None,
        },
        signals: MergeRequirementSignals {
            checks_known: true,
            ..Default::default()
        },
        reviews: Some(vec![]),
        threads: Some(ReviewThreadTally::default()),
        conversation_count: Some(0),
        availability: ReviewAvailability {
            policy: ProviderAvailability::Available,
            approvals: ProviderAvailability::Available,
            checks: ProviderAvailability::Available,
            discussions: ProviderAvailability::Available,
        },
    }
}
fn issue(number: u64) -> Issue {
    serde_json::from_value(json!({"number":number,"title":"issue","state":"open","url":"https://forge.example/item","author":"alice","createdAt":"","updatedAt":""})).unwrap()
}
fn keep<K: Clone + Eq + Hash, L, D>(cache: &mut CacheMap<K, L, D>) {
    super::super::retain(
        cache,
        |_| Some(Instant::now()),
        |_| false,
        (PR_CACHE_MAX_IDLE, PR_CACHE_MAX_ENTRIES),
        Instant::now(),
    );
}
fn page<T>(items: Vec<T>) -> ProviderRead<Page<T>> {
    (
        Ok(Page {
            items,
            next_cursor: Some("opaque cursor".into()),
        }),
        quota(),
    )
}
fn reviews(
    cache: &PrCache,
    request: &SummaryRequest<'_>,
    list: &ListObservations,
    numbers: &[u64],
) -> SummaryPage<PullRequest> {
    finish(
        cache,
        request,
        list,
        page(numbers.iter().copied().map(pull).collect()),
        |p| (target(request, p.number), true),
        keep,
    )
    .unwrap()
}
fn issues(
    cache: &IssueCache,
    request: &SummaryRequest<'_>,
    list: &ListObservations,
    numbers: &[u64],
) -> SummaryPage<Issue> {
    finish(
        cache,
        request,
        list,
        page(numbers.iter().copied().map(issue).collect()),
        |i| (target(request, i.number), true),
        keep,
    )
    .unwrap()
}
fn rejected_list(
    cache: &PrCache,
    request: &SummaryRequest<'_>,
    list: &ListObservations,
    reason: Ineligible,
) {
    let result = finish(
        cache,
        request,
        list,
        page(vec![pull(7)]),
        |p| (target(request, p.number), true),
        keep,
    )
    .unwrap_err();
    assert!(matches!(result.cause, CacheError::Ineligible(actual) if actual == reason));
    assert_eq!(result.quota, quota());
}
async fn read_detail(
    cache: &PrCache,
    request: &SummaryRequest<'_>,
    number: u64,
    max_age: Duration,
    result: intent_sourcecontrol::Result<ReviewObservation>,
) -> Result<CacheRead<ReviewObservation>, CacheFailure> {
    let target = target(request, number);
    let request = CacheRequest {
        connection: request.connection,
        target: &target,
        revalidate: request.revalidate,
    };
    super::super::read_review(
        cache,
        &request,
        PrReadPolicy::Serve { max_age },
        &NONE,
        || async { panic!("not a poll") },
        || async { (result, quota()) },
    )
    .await
}
async fn warm(cache: &PrCache, request: &SummaryRequest<'_>, number: u64) {
    read_detail(cache, request, number, Duration::ZERO, Ok(detail(number)))
        .await
        .unwrap();
}
async fn deny(
    cache: &PrCache,
    request: &SummaryRequest<'_>,
    number: u64,
    kind: ProviderFailureKind,
) {
    let error = read_detail(cache, request, number, Duration::ZERO, Err(denied(kind)))
        .await
        .unwrap_err();
    assert!(matches!(error.cause, CacheError::Provider(Error::Provider(e)) if e.kind==kind));
    assert_eq!(error.quota, quota());
}
fn numbers<T>(page: &SummaryPage<T>, number: impl Fn(&T) -> u64) -> Vec<u64> {
    page.page.items.iter().map(number).collect()
}

#[tokio::test]
async fn item_denial_after_list_start_fences_an_initially_unknown_row_only() {
    let connection = connection("alice");
    let repository = repository("team/app");
    let request = req(
        &connection,
        &repository,
        RepositoryResourceKind::MergeRequest,
    );
    let cache = PrCache::default();
    let list = begin(&request).unwrap();
    assert!(cache.lock().unwrap().is_empty());
    let (send, receive) = oneshot::channel();
    let work = async {
        receive.await.unwrap();
        reviews(&cache, &request, &list, &[9, 7, 8])
    };
    let denial = async {
        deny(&cache, &request, 7, ProviderFailureKind::ResourceDenied).await;
        send.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(work, denial);
    assert_eq!(numbers(&result, |p| p.number), [9, 8]);
    assert_eq!(result.omitted, 1);
    assert_eq!(result.quota, quota());
    assert_eq!(result.page.next_cursor.as_deref(), Some("opaque cursor"));
    for receipt in result.receipts.iter().flatten() {
        assert!(can_serve(&cache, &request, receipt, FRESH).unwrap());
    }
    let issue_request = req(&connection, &repository, RepositoryResourceKind::Issue);
    let issue_cache = IssueCache::default();
    assert_eq!(
        issues(
            &issue_cache,
            &issue_request,
            &begin(&issue_request).unwrap(),
            &[7]
        )
        .page
        .items
        .len(),
        1
    );
}

async fn broad_denial(kind: ProviderFailureKind) {
    let connection = connection("alice");
    let other = self::connection("bob");
    let repo = repository("team/app");
    let sibling = repository("team/other");
    let request = req(&connection, &repo, RepositoryResourceKind::MergeRequest);
    let issue_request = req(&connection, &repo, RepositoryResourceKind::Issue);
    let sibling_request = req(&connection, &sibling, RepositoryResourceKind::MergeRequest);
    let unrelated = req(&other, &repo, RepositoryResourceKind::MergeRequest);
    let cache = PrCache::default();
    let issue_cache = IssueCache::default();
    let lists = [&request, &issue_request, &sibling_request, &unrelated].map(|r| begin(r).unwrap());
    deny(&cache, &request, 99, kind).await;
    rejected_list(&cache, &request, &lists[0], Ineligible::DeniedSinceRequest);
    let issue_error = finish(
        &issue_cache,
        &issue_request,
        &lists[1],
        page(vec![issue(7)]),
        |i| (target(&issue_request, i.number), true),
        keep,
    )
    .unwrap_err();
    assert!(matches!(
        issue_error.cause,
        CacheError::Ineligible(Ineligible::DeniedSinceRequest)
    ));
    if kind == ProviderFailureKind::ProjectDenied {
        assert_eq!(
            reviews(&cache, &sibling_request, &lists[2], &[7])
                .page
                .items
                .len(),
            1
        );
    } else {
        rejected_list(
            &cache,
            &sibling_request,
            &lists[2],
            Ineligible::DeniedSinceRequest,
        );
    }
    assert_eq!(
        reviews(&cache, &unrelated, &lists[3], &[7])
            .page
            .items
            .len(),
        1
    );
}
#[tokio::test]
async fn connection_denial_fences_unknown_rows_across_projects_not_connections() {
    broad_denial(ProviderFailureKind::CredentialRejected).await;
}
#[tokio::test]
async fn project_denial_fences_unknown_mrs_and_issues_not_other_projects() {
    broad_denial(ProviderFailureKind::ProjectDenied).await;
}

#[tokio::test]
async fn fresh_summary_does_not_resurrect_denied_detail_or_a_pre_denial_list() {
    let connection = connection("alice");
    let repo = repository("team/app");
    let request = req(&connection, &repo, RepositoryResourceKind::MergeRequest);
    let cache = PrCache::default();
    warm(&cache, &request, 7).await;
    warm(&cache, &request, 8).await;
    let old = begin(&request).unwrap();
    deny(&cache, &request, 7, ProviderFailureKind::ProjectDenied).await;
    let fresh = reviews(&cache, &request, &begin(&request).unwrap(), &[7]);
    assert!(can_serve(&cache, &request, fresh.receipts[0].as_ref().unwrap(), FRESH).unwrap());
    rejected_list(&cache, &request, &old, Ineligible::DeniedSinceRequest);
    for number in [7, 8] {
        assert!(matches!(
            read_detail(
                &cache,
                &request,
                number,
                FRESH,
                Err(denied(ProviderFailureKind::Transient))
            )
            .await
            .unwrap_err()
            .cause,
            CacheError::Provider(_)
        ));
    }
    warm(&cache, &request, 7).await;
    assert!(
        !read_detail(
            &cache,
            &request,
            7,
            FRESH,
            Err(Error::Api("must hit".into()))
        )
        .await
        .unwrap()
        .fetched
    );
    assert!(read_detail(
        &cache,
        &request,
        8,
        FRESH,
        Err(Error::Api("still cold".into()))
    )
    .await
    .is_err());
}

#[tokio::test]
async fn eviction_and_reinsertion_cannot_admit_a_pending_unknown_row() {
    let connection = connection("alice");
    let repo = repository("team/app");
    let request = req(&connection, &repo, RepositoryResourceKind::MergeRequest);
    let cache = PrCache::default();
    let list = begin(&request).unwrap();
    warm(&cache, &request, 7).await;
    cache.lock().unwrap().clear();
    warm(&cache, &request, 7).await;
    assert_eq!(
        numbers(&reviews(&cache, &request, &list, &[7, 8]), |p| p.number),
        [8]
    );
    assert_eq!(
        reviews(&cache, &request, &begin(&request).unwrap(), &[7])
            .page
            .items
            .len(),
        1
    );
}

#[tokio::test]
async fn late_list_keeps_provider_order_but_cannot_replace_newer_same_item_summaries() {
    let connection = connection("alice");
    let repo = repository("team/app");
    let request = req(&connection, &repo, RepositoryResourceKind::MergeRequest);
    let cache = PrCache::default();
    let old = begin(&request).unwrap();
    let (send, receive) = oneshot::channel();
    let pending = async {
        receive.await.unwrap();
        reviews(&cache, &request, &old, &[9, 7, 6, 8])
    };
    let newer = async {
        let newer = reviews(&cache, &request, &begin(&request).unwrap(), &[8, 7]);
        send.send(()).unwrap();
        newer
    };
    let (old, newer) = tokio::join!(pending, newer);
    assert_eq!(numbers(&old, |p| p.number), [9, 6]);
    assert_eq!(old.omitted, 2);
    assert_eq!(numbers(&newer, |p| p.number), [8, 7]);
    for receipt in newer.receipts.iter().flatten() {
        assert!(can_serve(&cache, &request, receipt, FRESH).unwrap());
    }
}

#[test]
fn start_order_allows_a_newer_request_even_if_the_older_one_completed_first() {
    let connection = connection("alice");
    let repo = repository("team/app");
    let request = req(&connection, &repo, RepositoryResourceKind::MergeRequest);
    let cache = PrCache::default();
    let old = begin(&request).unwrap();
    let new = begin(&request).unwrap();
    let old = reviews(&cache, &request, &old, &[7]);
    let new = reviews(&cache, &request, &new, &[7]);
    assert!(!can_serve(&cache, &request, old.receipts[0].as_ref().unwrap(), FRESH).unwrap());
    assert!(can_serve(&cache, &request, new.receipts[0].as_ref().unwrap(), FRESH).unwrap());
}

#[tokio::test]
async fn partial_and_rate_limited_summary_fields_keep_values_quota_and_original_detail_age() {
    let connection = connection("alice");
    let repo = repository("team/app");
    let request = req(&connection, &repo, RepositoryResourceKind::MergeRequest);
    let cache = PrCache::default();
    warm(&cache, &request, 7).await;
    let key = CacheKey::Qualified(Box::new(connection.key(target(&request, 7))));
    let original = match &cache.lock().unwrap()[&key] {
        CacheSlot::Qualified(s) => s.payload.as_ref().unwrap().freshness.fetched_at,
        CacheSlot::Legacy(_) => unreachable!(),
    };
    let old = begin(&request).unwrap();
    for availability in [
        ProviderAvailability::Restricted,
        ProviderAvailability::Unknown,
        ProviderAvailability::Transient,
        ProviderAvailability::RateLimited,
    ] {
        let mut partial = detail(7);
        partial.reviews = None;
        partial.availability.approvals = availability;
        let list = begin(&request).unwrap();
        let result = finish(
            &cache,
            &request,
            &list,
            page(vec![partial.clone()]),
            |o| {
                (
                    target(&request, o.details.review.number),
                    complete_snapshot(o),
                )
            },
            keep,
        )
        .unwrap();
        assert_eq!(result.page.items, [partial]);
        assert!(result.receipts[0].is_none());
        assert_eq!(result.quota, quota());
    }
    assert!(reviews(&cache, &request, &old, &[7]).page.items.is_empty());
    let cache = cache.lock().unwrap();
    let CacheSlot::Qualified(slot) = &cache[&key] else {
        unreachable!()
    };
    assert_eq!(
        slot.payload.as_ref().unwrap().freshness.fetched_at,
        original
    );
    assert!(slot
        .observations
        .can_serve(&slot.payload.as_ref().unwrap().receipt, &connection));
}

#[tokio::test]
async fn list_endpoint_denial_does_not_become_item_or_project_denial() {
    let connection = connection("alice");
    let repo = repository("team/app");
    let request = req(&connection, &repo, RepositoryResourceKind::MergeRequest);
    let issue_request = req(&connection, &repo, RepositoryResourceKind::Issue);
    let cache = PrCache::default();
    let issue_cache = IssueCache::default();
    warm(&cache, &request, 7).await;
    let first = reviews(&cache, &request, &begin(&request).unwrap(), &[7]);
    let issue = issues(
        &issue_cache,
        &issue_request,
        &begin(&issue_request).unwrap(),
        &[7],
    );
    let pending = begin(&request).unwrap();
    let error = failure(
        &request,
        &pending,
        denied(ProviderFailureKind::ResourceDenied),
        quota(),
    );
    assert!(
        matches!(error.cause, CacheError::Provider(Error::Provider(e)) if e.kind == ProviderFailureKind::ResourceDenied)
    );
    assert_eq!(error.quota, quota());
    assert!(!can_serve(&cache, &request, first.receipts[0].as_ref().unwrap(), FRESH).unwrap());
    assert!(can_serve(
        &issue_cache,
        &issue_request,
        issue.receipts[0].as_ref().unwrap(),
        FRESH
    )
    .unwrap());
    assert!(
        !read_detail(
            &cache,
            &request,
            7,
            FRESH,
            Err(Error::Api("must hit".into()))
        )
        .await
        .unwrap()
        .fetched
    );
    rejected_list(&cache, &request, &pending, Ineligible::DeniedSinceRequest);
    assert_eq!(
        reviews(&cache, &request, &begin(&request).unwrap(), &[7])
            .page
            .items
            .len(),
        1
    );
}

#[tokio::test]
async fn non_denial_list_errors_preserve_original_error_quota_and_eligible_values() {
    let connection = connection("alice");
    let repo = repository("team/app");
    let request = req(&connection, &repo, RepositoryResourceKind::MergeRequest);
    let cache = PrCache::default();
    warm(&cache, &request, 7).await;
    let first = reviews(&cache, &request, &begin(&request).unwrap(), &[7]);
    let mut errors: Vec<_> = [
        ProviderFailureKind::OptionalRestricted,
        ProviderFailureKind::OptionalUnavailable,
        ProviderFailureKind::Transient,
        ProviderFailureKind::Unknown,
        ProviderFailureKind::WriteUncertain,
    ]
    .into_iter()
    .map(denied)
    .collect();
    errors.extend([
        Error::RateLimited("Retry-After: 90".into()),
        Error::Auth("local admission callback".into()),
        Error::NotFound("not classified".into()),
    ]);
    for error in errors {
        let expected = error.to_string();
        let result: Result<SummaryPage<PullRequest>, _> = finish(
            &cache,
            &request,
            &begin(&request).unwrap(),
            (Err(error), quota()),
            |p| (target(&request, p.number), true),
            keep,
        );
        let error = result.unwrap_err();
        let CacheError::Provider(error_value) = error.cause else {
            panic!("original provider error")
        };
        assert_eq!(error_value.to_string(), expected);
        assert_eq!(error.quota, quota());
        assert!(can_serve(&cache, &request, first.receipts[0].as_ref().unwrap(), FRESH).unwrap());
        assert!(
            !read_detail(
                &cache,
                &request,
                7,
                FRESH,
                Err(Error::Api("must hit".into()))
            )
            .await
            .unwrap()
            .fetched
        );
    }
}

#[test]
fn original_admission_is_checked_at_start_application_and_summary_hit() {
    let connection = connection("alice");
    let repo = repository("team/app");
    let authorized = AtomicBool::new(true);
    let check = || {
        if authorized.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(intent_core::Error::Internal("caller lost authority".into()))
        }
    };
    let request = SummaryRequest {
        revalidate: &check,
        ..req(&connection, &repo, RepositoryResourceKind::MergeRequest)
    };
    let cache = PrCache::default();
    let first = reviews(&cache, &request, &begin(&request).unwrap(), &[7]);
    let pending = begin(&request).unwrap();
    authorized.store(false, Ordering::SeqCst);
    assert!(matches!(
        begin(&request).unwrap_err().cause,
        CacheError::Admission(_)
    ));
    let result = finish(
        &cache,
        &request,
        &pending,
        page(vec![pull(8)]),
        |p| (target(&request, p.number), true),
        keep,
    );
    assert!(matches!(
        result.unwrap_err().cause,
        CacheError::Admission(_)
    ));
    assert!(matches!(
        can_serve(&cache, &request, first.receipts[0].as_ref().unwrap(), FRESH)
            .unwrap_err()
            .cause,
        CacheError::Admission(_)
    ));
    assert_eq!(cache.lock().unwrap().len(), 1);
}

#[test]
fn retired_or_same_ids_replacement_cannot_apply_old_success_or_denial() {
    let connection = connection("alice");
    let replacement = self::connection("alice");
    let repo = repository("team/app");
    let request = req(&connection, &repo, RepositoryResourceKind::MergeRequest);
    let new = req(&replacement, &repo, RepositoryResourceKind::MergeRequest);
    let cache = PrCache::default();
    let old = begin(&request).unwrap();
    let fresh = reviews(&cache, &new, &begin(&new).unwrap(), &[7]);
    let wrong = finish(
        &cache,
        &new,
        &old,
        page(vec![pull(7)]),
        |p| (target(&new, p.number), true),
        keep,
    )
    .unwrap_err();
    assert!(matches!(
        wrong.cause,
        CacheError::Ineligible(Ineligible::DifferentSlot)
    ));
    assert!(matches!(
        failure(
            &new,
            &old,
            denied(ProviderFailureKind::CredentialRejected),
            quota()
        )
        .cause,
        CacheError::Ineligible(Ineligible::DifferentSlot)
    ));
    connection.retire();
    assert!(matches!(
        failure(
            &request,
            &old,
            denied(ProviderFailureKind::CredentialRejected),
            quota()
        )
        .cause,
        CacheError::Ineligible(Ineligible::RetiredScope)
    ));
    assert!(can_serve(&cache, &new, fresh.receipts[0].as_ref().unwrap(), FRESH).unwrap());
}

#[tokio::test]
async fn bounded_history_expires_old_lists_without_revoking_unrelated_retained_receipts() {
    let connection = connection("alice");
    let repo = repository("team/app");
    let request = req(&connection, &repo, RepositoryResourceKind::MergeRequest);
    let cache = PrCache::default();
    let old = begin(&request).unwrap();
    let retained = reviews(&cache, &request, &begin(&request).unwrap(), &[1]);
    let key = CacheKey::Qualified(Box::new(connection.key(target(&request, 1))));
    if let CacheSlot::Qualified(slot) = cache.lock().unwrap().get_mut(&key).unwrap() {
        slot.monitored = true;
    }
    for number in 100..450 {
        deny(
            &cache,
            &request,
            number,
            ProviderFailureKind::ResourceDenied,
        )
        .await;
    }
    assert_eq!(
        old.validate_row(&target(&request, 2)),
        Err(Ineligible::HistoryExpired)
    );
    rejected_list(&cache, &request, &old, Ineligible::HistoryExpired);
    assert!(can_serve(
        &cache,
        &request,
        retained.receipts[0].as_ref().unwrap(),
        FRESH
    )
    .unwrap());
    assert!(cache.lock().unwrap().len() <= PR_CACHE_MAX_ENTRIES + 1);
    assert_eq!(
        reviews(&cache, &request, &begin(&request).unwrap(), &[2])
            .page
            .items
            .len(),
        1
    );
}

#[test]
fn summary_metadata_uses_existing_caps_and_never_contains_a_detail_payload() {
    let connection = connection("alice");
    let repo = repository("team/app");
    let request = req(&connection, &repo, RepositoryResourceKind::MergeRequest);
    let cache = PrCache::default();
    let first = reviews(
        &cache,
        &request,
        &begin(&request).unwrap(),
        &(0..200).collect::<Vec<_>>(),
    );
    let second = reviews(
        &cache,
        &request,
        &begin(&request).unwrap(),
        &(200..400).collect::<Vec<_>>(),
    );
    assert_eq!(cache.lock().unwrap().len(), PR_CACHE_MAX_ENTRIES);
    assert!(cache
        .lock()
        .unwrap()
        .values()
        .all(|slot| matches!(slot, CacheSlot::Qualified(s) if s.payload.is_none())));
    assert!(!can_serve(&cache, &request, first.receipts[0].as_ref().unwrap(), FRESH).unwrap());
    assert!(can_serve(
        &cache,
        &request,
        second.receipts[199].as_ref().unwrap(),
        FRESH
    )
    .unwrap());
    assert!(!can_serve(
        &cache,
        &request,
        second.receipts[199].as_ref().unwrap(),
        Duration::ZERO
    )
    .unwrap());
    let oversized = finish(
        &cache,
        &request,
        &begin(&request).unwrap(),
        page((0..201).map(pull).collect()),
        |p| (target(&request, p.number), true),
        keep,
    )
    .unwrap_err();
    assert!(matches!(oversized.cause, CacheError::PageTooLarge));
    assert_eq!(cache.lock().unwrap().len(), PR_CACHE_MAX_ENTRIES);
}

#[tokio::test]
async fn fresh_issue_summary_does_not_seed_issue_detail_and_item_denial_revokes_both() {
    let connection = connection("alice");
    let repo = repository("team/app");
    let request = req(&connection, &repo, RepositoryResourceKind::Issue);
    let cache = IssueCache::default();
    let result = issues(&cache, &request, &begin(&request).unwrap(), &[7]);
    let target = target(&request, 7);
    let detail_request = CacheRequest {
        connection: &connection,
        target: &target,
        revalidate: request.revalidate,
    };
    let full = read_qualified_issue(&cache, &detail_request, FRESH, || async {
        (Ok(issue(7)), quota())
    })
    .await
    .unwrap();
    assert!(full.fetched);
    let pending = begin(&request).unwrap();
    let error = read_qualified_issue(&cache, &detail_request, Duration::ZERO, || async {
        (Err(denied(ProviderFailureKind::ResourceDenied)), quota())
    })
    .await
    .unwrap_err();
    assert!(matches!(error.cause, CacheError::Provider(_)));
    assert!(!can_serve(
        &cache,
        &request,
        result.receipts[0].as_ref().unwrap(),
        FRESH
    )
    .unwrap());
    assert!(issues(&cache, &request, &pending, &[7])
        .page
        .items
        .is_empty());
    assert_eq!(
        issues(&cache, &request, &begin(&request).unwrap(), &[7])
            .page
            .items
            .len(),
        1
    );
}

#[test]
fn mixed_project_rows_use_original_captures_without_reordering_or_widening_denial() {
    let connection = connection("alice");
    let first = repository("team/one");
    let second = repository("team/two");
    let first = req(&connection, &first, RepositoryResourceKind::MergeRequest);
    let second = req(&connection, &second, RepositoryResourceKind::MergeRequest);
    let captures = [begin(&first).unwrap(), begin(&second).unwrap()];
    let _error = failure(
        &first,
        &captures[0],
        denied(ProviderFailureKind::ProjectDenied),
        quota(),
    );
    let cache = PrCache::default();
    let mut cache = cache.lock().unwrap();
    let mut kept = Vec::new();
    for (request, capture, number) in [
        (&second, &captures[1], 9),
        (&first, &captures[0], 8),
        (&second, &captures[1], 7),
    ] {
        if row(
            &mut cache,
            request,
            capture,
            &target(request, number),
            true,
            quota(),
        )
        .is_ok()
        {
            kept.push(number);
        }
    }
    assert_eq!(kept, [9, 7]);
    assert!(row(
        &mut cache,
        &first,
        &captures[1],
        &target(&second, 6),
        true,
        quota()
    )
    .is_err());
}

#[test]
fn github_summary_identity_never_uses_an_unqualified_legacy_slot() {
    let connection = connection("alice");
    let mut repo = repository("owner/repo");
    repo.provider = RepositoryProvider::Github;
    repo.instance_base_url = "https://github.com".into();
    let request = req(&connection, &repo, RepositoryResourceKind::PullRequest);
    let cache = PrCache::default();
    cache.lock().unwrap().insert(
        CacheKey::Legacy(("owner".into(), "repo".into(), 7)),
        CacheSlot::Legacy(LegacyPrCacheSlot::default()),
    );
    let result = reviews(&cache, &request, &begin(&request).unwrap(), &[7]);
    assert_eq!(cache.lock().unwrap().len(), 2);
    assert!(can_serve(
        &cache,
        &request,
        result.receipts[0].as_ref().unwrap(),
        FRESH
    )
    .unwrap());
    assert!(cache.lock().unwrap().contains_key(&CacheKey::Legacy((
        "owner".into(),
        "repo".into(),
        7
    ))));
}

#[tokio::test]
async fn history_overrun_and_fresh_recovery_never_revive_a_denied_retained_receipt() {
    let connection = connection("alice");
    let repo = repository("team/app");
    let request = req(&connection, &repo, RepositoryResourceKind::MergeRequest);
    let cache = PrCache::default();
    let retained = reviews(&cache, &request, &begin(&request).unwrap(), &[1]);
    let receipt = retained.receipts[0].as_ref().unwrap();
    let key = CacheKey::Qualified(Box::new(connection.key(target(&request, 1))));
    if let CacheSlot::Qualified(slot) = cache.lock().unwrap().get_mut(&key).unwrap() {
        slot.monitored = true;
    }
    deny(&cache, &request, 1, ProviderFailureKind::ResourceDenied).await;
    // The denial record itself will fall outside the bounded history. The
    // retained receipt must still fail its independent slot/denial proof.
    for number in 100..450 {
        deny(
            &cache,
            &request,
            number,
            ProviderFailureKind::ResourceDenied,
        )
        .await;
    }
    assert!(!can_serve(&cache, &request, receipt, FRESH).unwrap());
    warm(&cache, &request, 1).await;
    assert!(
        !can_serve(&cache, &request, receipt, FRESH).unwrap(),
        "detail recovery cannot revive summary coverage"
    );
    let recovered = reviews(&cache, &request, &begin(&request).unwrap(), &[1]);
    assert!(can_serve(
        &cache,
        &request,
        recovered.receipts[0].as_ref().unwrap(),
        FRESH
    )
    .unwrap());
    assert!(!can_serve(&cache, &request, receipt, FRESH).unwrap());
    cache.lock().unwrap().remove(&key);
    assert!(!can_serve(
        &cache,
        &request,
        recovered.receipts[0].as_ref().unwrap(),
        FRESH
    )
    .unwrap());
    let reinserted = reviews(&cache, &request, &begin(&request).unwrap(), &[1]);
    assert!(can_serve(
        &cache,
        &request,
        reinserted.receipts[0].as_ref().unwrap(),
        FRESH
    )
    .unwrap());
    assert!(!can_serve(
        &cache,
        &request,
        recovered.receipts[0].as_ref().unwrap(),
        FRESH
    )
    .unwrap());
}

#[tokio::test]
async fn owned_provider_query_is_captured_before_dispatch_and_unknown_row_denial() {
    let connection = connection("alice");
    let repo = repository("team/app");
    let request = req(&connection, &repo, RepositoryResourceKind::MergeRequest);
    let cache = PrCache::default();
    let mut query = intent_sourcecontrol::PrQuery {
        search: Some("original".into()),
        limit: Some(50),
        cursor: Some("original cursor".into()),
        ..Default::default()
    };
    let (arrived, arrival) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let read = super::read(
        &cache,
        &request,
        query.clone(),
        |captured| async move {
            arrived.send(()).unwrap();
            released.await.unwrap();
            assert_eq!(captured.search.as_deref(), Some("original"));
            assert_eq!(captured.limit, Some(50));
            assert_eq!(captured.cursor.as_deref(), Some("original cursor"));
            page(vec![pull(7), pull(8)])
        },
        |p| (target(&request, p.number), true),
        keep,
    );
    let replacement = async {
        arrival.await.unwrap();
        query.search = Some("different".into());
        query.cursor = None;
        deny(&cache, &request, 7, ProviderFailureKind::ResourceDenied).await;
        release.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(read, replacement);
    let result = result.unwrap();
    assert_eq!(numbers(&result, |p| p.number), [8]);
    assert_eq!(result.omitted, 1);
    assert_eq!(result.page.next_cursor.as_deref(), Some("opaque cursor"));
    assert_eq!(result.quota, quota());
}

#[tokio::test]
async fn empty_pages_also_expire_when_pending_history_is_lost() {
    let connection = connection("alice");
    let repo = repository("team/app");
    let request = req(&connection, &repo, RepositoryResourceKind::MergeRequest);
    let cache = PrCache::default();
    let old = begin(&request).unwrap();
    for number in 0..300 {
        deny(
            &cache,
            &request,
            number,
            ProviderFailureKind::ResourceDenied,
        )
        .await;
    }
    let result = finish(
        &cache,
        &request,
        &old,
        page(Vec::<PullRequest>::new()),
        |p| (target(&request, p.number), true),
        keep,
    )
    .unwrap_err();
    assert!(matches!(
        result.cause,
        CacheError::Ineligible(Ineligible::HistoryExpired)
    ));
    assert_eq!(result.quota, quota());
    let fresh = finish(
        &cache,
        &request,
        &begin(&request).unwrap(),
        page(Vec::<PullRequest>::new()),
        |p| (target(&request, p.number), true),
        keep,
    )
    .unwrap();
    assert!(fresh.page.items.is_empty());
    assert_eq!(fresh.page.next_cursor.as_deref(), Some("opaque cursor"));
}
