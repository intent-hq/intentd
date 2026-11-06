//! One explicitly admitted project/page on the original scoped read session.
//! Provider values are never a grant, and no page payload is retained here.

use intent_core::repository_resource_pages::{
    RepositoryDiscussions, RepositoryDiscussionsQuery, RepositoryListFilter, RepositoryListKind,
    RepositoryListState, RepositoryPageFailure, RepositoryPageOutcome, RepositoryResourceList,
    RepositoryResourceListQuery,
};
use intent_core::RepositoryTarget;
use intent_sourcecontrol::{
    IssueInvolvement, IssueQuery, Page, PageParams, PrInvolvement, PrQuery, PrState,
    RateLimitStatus,
};

use super::{
    credentials, cursors, denied, entry, failure, observations, read_limit, unavailable,
    valid_repository, valid_target, validate_resource_url, Arc, AtomicBool, BoxFuture,
    CacheAdmissionGuard, CacheDelivery, CacheError, CacheFailure, CacheRequest, Frame, Lease,
    ManagedCacheRequest, Mutex, Ordering, Quota, ReadAuthority, RepositoryAuthorityRequest,
    RepositoryCollectionTarget, RepositoryCredentialTransport, RepositoryCredentialUse,
    RepositoryReadEligibility, RepositoryReadOperation, RepositoryResourceKind,
    RepositoryResponseAttribution, Request, Result, ReviewTarget, Services, State,
};
use crate::pr_monitor::qualified_cache::{summaries, CacheDenialObserver};
use crate::repository_credentials::read::{
    RepositoryCollectionQuery, RepositoryCollectionReadOperation,
};

pub(crate) fn list(
    services: &Services,
    q: RepositoryResourceListQuery,
) -> BoxFuture<'_, Result<RepositoryResourceList>> {
    entry(services, &Frame::List(q.clone()), move |r| {
        Box::pin(async move {
            valid_repository(&q.repository)?;
            let lease = r.lease(&q.read_lifetime_id)?;
            if !lease.group.supports_pages() {
                return Err(unavailable());
            }
            *r.state.lock().map_err(denied)? = State::Reading(lease.clone());
            let stop = r
                .base
                .lifetime
                .as_ref()
                .ok_or_else(unavailable)?
                .retirement();
            let lease_stop = lease.lifetime.retirement();
            tokio::select! {
                biased;
                ()=stop.native_cancelled()=>Err(unavailable()),
                ()=lease_stop.native_cancelled()=>Err(unavailable()),
                value=tokio::time::timeout(read_limit(), list_inner(&r, &lease, q))=>value.map_err(denied)?,
            }
        })
    })
}

struct Admission {
    authority: Arc<ReadAuthority>,
    eligibility: Arc<RepositoryReadEligibility>,
    original: credentials::RepositoryCredentialAdmission,
}

fn admit(r: &Arc<Request>, lease: &Arc<Lease>, repository: &RepositoryTarget) -> Result<Admission> {
    if repository.instance_base_url != lease.settled.descriptor().instance().as_str() {
        return Err(unavailable());
    }
    let expected = RepositoryAuthorityRequest {
        execution: lease.scope.clone(),
        target: repository.clone(),
        connection: lease.settled.selected().binding.scope.clone(),
        use_kind: RepositoryCredentialUse::NativeRead,
        allowed_transport: RepositoryCredentialTransport::GitlabApi(
            lease.settled.descriptor().clone(),
        ),
    };
    let authority = Arc::new(ReadAuthority {
        request: Arc::downgrade(r),
        lease: lease.clone(),
        expected: expected.clone(),
    });
    let original = r
        .base
        .connection
        .services
        .repository_connection_directory()
        .admit(
            &lease.settled.selected().binding,
            expected,
            Arc::new(authority.clone()),
        )
        .map_err(denied)?;
    let eligibility = Arc::new(
        r.base
            .connection
            .services
            .gitlab_repository_read_eligibility(&original)
            .map_err(denied)?,
    );
    Ok(Admission {
        authority,
        eligibility,
        original,
    })
}

fn continuation(
    lease: &Lease,
    key: &cursors::QueryKey,
    token: Option<&str>,
    authority: &ReadAuthority,
    eligibility: &RepositoryReadEligibility,
) -> Result<Option<String>> {
    let Some(token) = token else { return Ok(None) };
    let original = lease.cursors.lock().map_err(denied)?.get(key, token)?;
    original
        .delivery
        .with_revalidation(&mut |check| {
            authority.with_current(|| eligibility.with_current(&mut || check()))
        })
        .map_err(denied)?;
    Ok(Some(original.provider))
}

fn review_query(q: &RepositoryResourceListQuery, cursor: Option<String>) -> PrQuery {
    PrQuery {
        state: match q.query.state {
            RepositoryListState::All => None,
            RepositoryListState::Open => Some(PrState::Open),
            RepositoryListState::Closed => Some(PrState::Closed),
            RepositoryListState::Merged => Some(PrState::Merged),
        },
        involvement: match q.query.filter {
            RepositoryListFilter::All => None,
            RepositoryListFilter::Created => Some(PrInvolvement::Created),
            RepositoryListFilter::Assigned => Some(PrInvolvement::Assigned),
            RepositoryListFilter::ReviewRequested => Some(PrInvolvement::ReviewRequested),
            RepositoryListFilter::Involves => Some(PrInvolvement::Involves),
        },
        search: Some(q.query.search.clone()),
        limit: Some(q.query.limit),
        cursor,
        ..Default::default()
    }
}

async fn list_inner(
    r: &Arc<Request>,
    lease: &Arc<Lease>,
    q: RepositoryResourceListQuery,
) -> Result<RepositoryResourceList> {
    let c = &r.base.connection;
    let _legacy = c.caller.legacy_lease().await.map_err(denied)?;
    r.validate(lease).await?;
    let Admission {
        authority,
        eligibility,
        original,
    } = admit(r, lease, &q.repository)?;
    let key = cursors::QueryKey::list(&q.repository, q.kind, &q.query);
    let cursor = continuation(
        lease,
        &key,
        q.query.cursor.as_deref(),
        &authority,
        &eligibility,
    )?;
    // These combinations have no exact GitLab issue semantics. An explicit
    // unsupported result is neither an empty page nor an authentication denial.
    if q.kind == RepositoryListKind::Issue
        && (q.query.state == RepositoryListState::Merged
            || matches!(
                q.query.filter,
                RepositoryListFilter::ReviewRequested | RepositoryListFilter::Involves
            ))
    {
        *r.state.lock().map_err(denied)? =
            State::Read(lease.clone(), eligibility, None, false, None, None);
        return Ok(RepositoryResourceList {
            context: q.context,
            read_lifetime_id: lease.id.clone(),
            scope: lease.scope.clone(),
            revision: lease.revision(),
            repository: q.repository,
            kind: q.kind,
            query: q.query,
            outcome: RepositoryPageOutcome::Failure {
                code: RepositoryPageFailure::Unsupported,
                status: None,
            },
            quota: quota(RateLimitStatus::default()),
        });
    }
    let query = match q.kind {
        RepositoryListKind::MergeRequest => {
            RepositoryCollectionQuery::Reviews(review_query(&q, cursor))
        }
        RepositoryListKind::Issue => {
            let query = IssueQuery {
                state: match q.query.state {
                    RepositoryListState::All => None,
                    RepositoryListState::Open => Some("open".into()),
                    RepositoryListState::Closed => Some("closed".into()),
                    RepositoryListState::Merged => unreachable!(),
                },
                search: Some(q.query.search.clone()),
                limit: Some(q.query.limit),
                cursor,
                ..Default::default()
            };
            match q.query.filter {
                RepositoryListFilter::Created => RepositoryCollectionQuery::FilteredIssues {
                    query,
                    involvement: IssueInvolvement::Created,
                },
                RepositoryListFilter::Assigned => RepositoryCollectionQuery::FilteredIssues {
                    query,
                    involvement: IssueInvolvement::Assigned,
                },
                RepositoryListFilter::All => RepositoryCollectionQuery::Issues(query),
                RepositoryListFilter::ReviewRequested | RepositoryListFilter::Involves => {
                    unreachable!()
                }
            }
        }
    };
    let operation = RepositoryCollectionReadOperation::new(
        c.services.repository_connection_directory(),
        original,
        c.services
            .gitlab_repository_secret_reader()
            .map_err(denied)?,
        read_limit(),
        query,
    )
    .map_err(denied)?;
    let target = operation.target().clone();
    let denials = observations::Denials::new(r, lease);
    let guard: &CacheAdmissionGuard<'_> = &|action| denials.with_current(action);
    let observer: &CacheDenialObserver<'_> = &|notice| denials.observe(notice);
    let revalidate = || r.check().and_then(|()| lease.check());
    let monitored = crate::pr_monitor::qualified_cache::monitored_review_keys(&c.services).await;
    let managed = summaries::ManagedSummaryRequest {
        request: summaries::SummaryRequest {
            connection: &lease.observations,
            repository: &q.repository,
            kind: q.kind.resource_kind(),
            revalidate: &revalidate,
        },
        target: &target,
        eligibility: &eligibility,
        with_authority: guard,
        denials: observer,
    };
    let response = Mutex::new(None);
    let invalid = AtomicBool::new(false);
    let addressed = |number| ReviewTarget {
        repository: q.repository.clone(),
        kind: q.kind.resource_kind(),
        number,
    };
    let matches = |number, url: &str| {
        valid_target(&addressed(number)).is_ok()
            && validate_resource_url(&addressed(number), url).is_ok()
    };
    let result = match q.kind {
        RepositoryListKind::MergeRequest => summaries::read_managed(
            &c.services.pr_cache,
            &managed,
            || async {
                let v = operation.reviews().await.reject_value_unless(|page| {
                    let ok = page.items.len() <= usize::from(q.query.limit)
                        && page
                            .items
                            .iter()
                            .all(|item| matches(item.number, &item.url));
                    invalid.store(!ok, Ordering::Release);
                    ok
                });
                *response.lock().unwrap() = Some(v.attribution());
                let _ = authority.validate().await;
                v
            },
            |item| (addressed(item.number), true),
            |cache| {
                crate::pr_monitor::qualified_cache::retain_pr_cache(
                    cache,
                    &monitored,
                    std::time::Instant::now(),
                );
            },
        )
        .await
        .map(|page| {
            (
                serialize(page.page),
                page.quota,
                page.delivery,
                page.omitted,
            )
        }),
        RepositoryListKind::Issue => summaries::read_managed(
            &c.services.issue_cache,
            &managed,
            || async {
                let v = operation.issues().await.reject_value_unless(|page| {
                    let ok = page.items.len() <= usize::from(q.query.limit)
                        && page
                            .items
                            .iter()
                            .all(|item| matches(item.number, &item.url));
                    invalid.store(!ok, Ordering::Release);
                    ok
                });
                *response.lock().unwrap() = Some(v.attribution());
                let _ = authority.validate().await;
                v
            },
            |item| (addressed(item.number), true),
            |cache| crate::issue_cache::retain_issue_cache(cache, std::time::Instant::now()),
        )
        .await
        .map(|page| {
            (
                serialize(page.page),
                page.quota,
                page.delivery,
                page.omitted,
            )
        }),
    };
    if invalid.load(Ordering::Acquire) {
        return Err(unavailable());
    }
    r.validate(lease).await?;
    let (outcome, rate) = finish(
        r,
        lease,
        key,
        eligibility,
        response.into_inner().map_err(denied)?,
        Some(Arc::new(target)),
        result,
    )?;
    Ok(RepositoryResourceList {
        context: q.context,
        read_lifetime_id: lease.id.clone(),
        scope: lease.scope.clone(),
        revision: lease.revision(),
        repository: q.repository,
        kind: q.kind,
        query: q.query,
        outcome,
        quota: rate,
    })
}

type SerializedPage = std::result::Result<Page<serde_json::Value>, serde_json::Error>;
type PageRead =
    std::result::Result<(SerializedPage, RateLimitStatus, CacheDelivery, usize), CacheFailure>;

fn serialize<T: serde::Serialize>(page: Page<T>) -> SerializedPage {
    Ok(Page {
        items: page
            .items
            .into_iter()
            .map(serde_json::to_value)
            .collect::<std::result::Result<Vec<_>, _>>()?,
        next_cursor: page.next_cursor,
    })
}

fn finish(
    r: &Request,
    lease: &Arc<Lease>,
    key: cursors::QueryKey,
    eligibility: Arc<RepositoryReadEligibility>,
    response: Option<Arc<RepositoryResponseAttribution>>,
    collection: Option<Arc<RepositoryCollectionTarget>>,
    result: PageRead,
) -> Result<(RepositoryPageOutcome, Quota)> {
    let (outcome, rate, failed, delivery) = match result {
        Ok((page, rate, delivery, omitted)) => {
            let page = page.map_err(denied)?;
            let next_cursor =
                lease
                    .cursors
                    .lock()
                    .map_err(denied)?
                    .insert(key, page.next_cursor, &delivery)?;
            (
                RepositoryPageOutcome::Page {
                    items: page.items,
                    next_cursor,
                    omitted,
                },
                rate,
                false,
                Some(delivery),
            )
        }
        Err(CacheFailure {
            cause: CacheError::Provider(error),
            quota,
        }) => {
            let (code, status) = match &error {
                intent_sourcecontrol::Error::Unsupported(_)
                | intent_sourcecontrol::Error::DeviceGrantUnsupported(_) => {
                    (RepositoryPageFailure::Unsupported, None)
                }
                _ => {
                    let (code, status) = failure(&error);
                    (code.into(), status)
                }
            };
            (
                RepositoryPageOutcome::Failure { code, status },
                quota,
                true,
                None,
            )
        }
        Err(_) => return Err(unavailable()),
    };
    *r.state.lock().map_err(denied)? = State::Read(
        lease.clone(),
        eligibility,
        response,
        failed,
        delivery,
        collection,
    );
    Ok((outcome, quota(rate)))
}

fn quota(rate: RateLimitStatus) -> Quota {
    Quota {
        reset_at: rate.reset_at.map(|n| n.to_string()),
        remaining: rate.remaining.map(|n| n.to_string()),
        limit: rate.limit.map(|n| n.to_string()),
    }
}

pub(crate) fn discussions(
    services: &Services,
    q: RepositoryDiscussionsQuery,
) -> BoxFuture<'_, Result<RepositoryDiscussions>> {
    entry(services, &Frame::Discussions(q.clone()), move |r| {
        Box::pin(async move {
            valid_target(&q.target)?;
            if q.target.kind != RepositoryResourceKind::MergeRequest {
                return Err(unavailable());
            }
            let lease = r.lease(&q.read_lifetime_id)?;
            if !lease.group.supports_pages() {
                return Err(unavailable());
            }
            *r.state.lock().map_err(denied)? = State::Reading(lease.clone());
            let stop = r
                .base
                .lifetime
                .as_ref()
                .ok_or_else(unavailable)?
                .retirement();
            let lease_stop = lease.lifetime.retirement();
            tokio::select! {
                biased;
                ()=stop.native_cancelled()=>Err(unavailable()),
                ()=lease_stop.native_cancelled()=>Err(unavailable()),
                value=tokio::time::timeout(read_limit(),discussions_inner(&r,&lease,q))=>value.map_err(denied)?,
            }
        })
    })
}

async fn discussions_inner(
    r: &Arc<Request>,
    lease: &Arc<Lease>,
    q: RepositoryDiscussionsQuery,
) -> Result<RepositoryDiscussions> {
    let c = &r.base.connection;
    let _legacy = c.caller.legacy_lease().await.map_err(denied)?;
    r.validate(lease).await?;
    let Admission {
        authority,
        eligibility,
        original,
    } = admit(r, lease, &q.target.repository)?;
    let key = cursors::QueryKey::discussions(&q.target, &q.page);
    let cursor = continuation(
        lease,
        &key,
        q.page.cursor.as_deref(),
        &authority,
        &eligibility,
    )?;
    let operation = RepositoryReadOperation::new(
        c.services.repository_connection_directory(),
        original,
        c.services
            .gitlab_repository_secret_reader()
            .map_err(denied)?,
        read_limit(),
        q.target.clone(),
    )
    .map_err(denied)?;
    let denials = observations::Denials::new(r, lease);
    let guard: &CacheAdmissionGuard<'_> = &|action| denials.with_current(action);
    let observer: &CacheDenialObserver<'_> = &|notice| denials.observe(notice);
    let revalidate = || r.check().and_then(|()| lease.check());
    let managed = ManagedCacheRequest {
        request: CacheRequest {
            connection: &lease.observations,
            target: &q.target,
            revalidate: &revalidate,
        },
        eligibility: &eligibility,
        with_authority: guard,
    };
    let response = Mutex::new(None);
    let monitored = crate::pr_monitor::qualified_cache::monitored_review_keys(&c.services).await;
    let result = crate::pr_monitor::qualified_cache::read_managed_review_section_observed(
        &c.services.pr_cache,
        &managed,
        observer,
        &monitored,
        || async {
            let v = operation
                .review_discussions(PageParams {
                    limit: q.page.limit,
                    cursor,
                })
                .await
                .reject_value_unless(|page| page.items.len() <= usize::from(q.page.limit));
            *response.lock().unwrap() = Some(v.attribution());
            let _ = authority.validate().await;
            v
        },
    )
    .await
    .map(|read| (serialize(read.value), read.quota, read.delivery, 0));
    r.validate(lease).await?;
    let (outcome, rate) = finish(
        r,
        lease,
        key,
        eligibility,
        response.into_inner().map_err(denied)?,
        None,
        result,
    )?;
    Ok(RepositoryDiscussions {
        context: q.context,
        read_lifetime_id: lease.id.clone(),
        scope: lease.scope.clone(),
        revision: lease.revision(),
        target: q.target,
        page: q.page,
        outcome,
        quota: rate,
    })
}
