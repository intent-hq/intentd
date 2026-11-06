//! Receipts for the existing one-page list/suggestion flow, not a page cache.
//!
//! Legacy list/search handlers return a provider Page directly; they do not
//! populate the detail maps. The renderer owns its existing first-page retention.
//! These private primitives qualify rows using the SAME bounded resource slots
//! as detail reads. A summary never fills or refreshes a detail payload.
//!
//! Capture each explicitly admitted project's request before dispatch. A blended
//! result needs those captures and confirmed per-row targets; never rediscover
//! scope from the current workspace or infer error origin from a returned URL.
//! Caller/wire activation and provider error attribution are separate work.

use std::future::Future;
use std::hash::Hash;
use std::time::{Duration, Instant};

use crate::repository_credentials::read::{
    RepositoryCollectionTarget, RepositoryProviderRead, RepositoryResponseDisposition,
};
use crate::repository_credentials::RepositoryCredentialError;
use crate::source_control_auth_ops::repository_owner::RepositoryReadEligibility;
use intent_core::{RepositoryResourceKind, RepositoryTarget, ReviewTarget};
use intent_sourcecontrol::{Page, RateLimitStatus};

use super::{
    new_qualified_slot, new_slot, observe_denial, page_delivery, CacheAdmissionGuard,
    CacheDelivery, CacheDenialCoverage, CacheDenialObserver, CacheError, CacheFailure, CacheKey,
    CacheMap, CacheRequest, CacheSlot, ProviderRead, SharedCache,
};
use crate::observation_adapter::{
    ConnectionObservations, ListObservations, ListReceipt, ObservationReceipt, QualifiedKey,
};
use crate::observation_policy::{Coverage, Ineligible};

pub(crate) struct SummaryRequest<'a> {
    pub(crate) connection: &'a ConnectionObservations,
    pub(crate) repository: &'a RepositoryTarget,
    pub(crate) kind: RepositoryResourceKind,
    pub(crate) revalidate: &'a (dyn Fn() -> intent_core::Result<()> + Sync),
}

impl SummaryRequest<'_> {
    fn check(&self, quota: RateLimitStatus) -> Result<(), CacheFailure> {
        (self.revalidate)().map_err(|e| CacheFailure {
            cause: CacheError::Admission(e),
            quota,
        })?;
        self.check_metadata(quota)
    }

    fn check_metadata(&self, quota: RateLimitStatus) -> Result<(), CacheFailure> {
        if !self.connection.is_active() {
            return Err(CacheFailure::ineligible(Ineligible::RetiredScope, quota));
        }
        Ok(())
    }

    fn matches(&self, list: &ListObservations, quota: RateLimitStatus) -> Result<(), CacheFailure> {
        self.check(quota)?;
        self.matches_metadata(list, quota)
    }

    fn matches_metadata(
        &self,
        list: &ListObservations,
        quota: RateLimitStatus,
    ) -> Result<(), CacheFailure> {
        self.check_metadata(quota)?;
        if !list.matches(self.connection, self.repository, self.kind) {
            return Err(CacheFailure::ineligible(Ineligible::DifferentSlot, quota));
        }
        Ok(())
    }
}

/// Opaque metadata only; payloads stay with the existing consumer and TTL.
#[derive(Debug)]
pub(crate) struct SummaryReceipt {
    key: QualifiedKey,
    observation: ObservationReceipt,
    list: ListReceipt,
    fetched_at: Instant,
}

/// Same page order/cursor and provider values. Ineligible rows are omitted and
/// counted. A partial row is returned unchanged with no retention receipt.
#[derive(Debug)]
pub(crate) struct SummaryPage<T> {
    pub(crate) page: Page<T>,
    pub(crate) receipts: Vec<Option<SummaryReceipt>>,
    pub(crate) omitted: usize,
    pub(crate) quota: RateLimitStatus,
    /// Original request and row eligibility through final caller/provider transfer.
    /// The existing bounded response owner drops this after delivery or expiry.
    pub(crate) delivery: CacheDelivery,
}

pub(crate) fn begin(request: &SummaryRequest<'_>) -> Result<ListObservations, CacheFailure> {
    let quota = RateLimitStatus::default();
    request.check(quota)?;
    let list = request
        .connection
        .project(request.repository.clone())
        .begin_list(request.kind)
        .map_err(|e| CacheFailure::ineligible(e, quota))?;
    request.check(quota)?;
    Ok(list)
}

/// Apply an attributed list failure before `map_sc_err` loses purpose. A mixed
/// project operation must identify the actual failing call; its first/default
/// project is NOT an acceptable substitute for that evidence.
pub(crate) fn failure(
    request: &SummaryRequest<'_>,
    list: &ListObservations,
    error: intent_sourcecontrol::Error,
    quota: RateLimitStatus,
) -> CacheFailure {
    if request.matches(list, quota).is_ok() {
        let kind = match &error {
            intent_sourcecontrol::Error::Provider(failure) => Some(failure.kind),
            _ => None,
        };
        let _ = list.failure_kind(kind);
    }
    // This response has already arrived. A local refusal may suppress its
    // cache effect, but it must not replace the provider's error or quota.
    CacheFailure {
        cause: CacheError::Provider(error),
        quota,
    }
}

/// Qualify one row while holding its existing cache map's lock. Exposed for a
/// blended page's existing owner to preserve its ordering while choosing each
/// row's PRE-CAPTURED project receipt. This does not resolve or parse identities.
pub(crate) fn row<K: Clone + Eq + Hash, L, D>(
    cache: &mut CacheMap<K, L, D>,
    request: &SummaryRequest<'_>,
    list: &ListObservations,
    target: &ReviewTarget,
    complete: bool,
    quota: RateLimitStatus,
) -> Result<Option<SummaryReceipt>, CacheFailure> {
    request.matches(list, quota)?;
    row_in_action(cache, request, list, target, complete, quota)
}

fn row_in_action<K: Clone + Eq + Hash, L, D>(
    cache: &mut CacheMap<K, L, D>,
    request: &SummaryRequest<'_>,
    list: &ListObservations,
    target: &ReviewTarget,
    complete: bool,
    quota: RateLimitStatus,
) -> Result<Option<SummaryReceipt>, CacheFailure> {
    request.matches_metadata(list, quota)?;
    list.validate_row(target)
        .map_err(|e| CacheFailure::ineligible(e, quota))?;
    let detail_request = CacheRequest {
        connection: request.connection,
        target,
        revalidate: request.revalidate,
    };
    let key = request.connection.key(target.clone());
    let CacheSlot::Qualified(slot) = cache
        .entry(CacheKey::Qualified(Box::new(key.clone())))
        .or_insert_with(|| new_slot(&detail_request))
    else {
        unreachable!("qualified key")
    };
    if !slot.observations.belongs_to(request.connection) {
        **slot = new_qualified_slot(&detail_request);
    }
    let ticket = slot
        .observations
        .begin(Coverage::Summary)
        .map_err(|e| CacheFailure::ineligible(e, quota))?;
    // The per-item ticket is allocated after await, so it is insufficient by
    // itself: both request-start scope and unknown-item history must still hold.
    list.validate_row(target)
        .map_err(|e| CacheFailure::ineligible(e, quota))?;
    request.check_metadata(quota)?;
    let receipt = if complete {
        Some(SummaryReceipt {
            key,
            observation: slot
                .observations
                .primary_success(ticket)
                .map_err(|e| CacheFailure::ineligible(e, quota))?,
            list: list.receipt(),
            fetched_at: Instant::now(),
        })
    } else {
        slot.observations
            .partial_success(&ticket)
            .map_err(|e| CacheFailure::ineligible(e, quota))?;
        None
    };
    list.observed(target.clone());
    Ok(receipt)
}

/// One addressed provider page, within the existing RPC's maximum page size.
/// The caller's clamped query limit and pagination behavior remain unchanged.
/// `describe` supplies confirmed canonical targets and summary completeness;
/// optional detail fields do not become complete simply because a list arrived.
pub(crate) fn finish<
    K: Clone + Eq + Hash + Send + Sync + 'static,
    L: Send + 'static,
    D: Send + 'static,
    T,
>(
    cache: &SharedCache<K, L, D>,
    request: &SummaryRequest<'_>,
    list: &ListObservations,
    outcome: ProviderRead<Page<T>>,
    describe: impl Fn(&T) -> (ReviewTarget, bool),
    retain: impl Fn(&mut CacheMap<K, L, D>),
) -> Result<SummaryPage<T>, CacheFailure> {
    let (outcome, quota) = outcome;
    let page = outcome.map_err(|error| failure(request, list, error, quota))?;
    request.matches(list, quota)?;
    // Empty pages/cursors also belong to the original request. An empty late
    // response must not clear a newer page after losing its scope/history proof.
    list.validate_start()
        .map_err(|e| CacheFailure::ineligible(e, quota))?;
    if page.items.len() > usize::from(crate::github_ops::clamp_limit(Some(i64::MAX))) {
        return Err(CacheFailure {
            cause: CacheError::PageTooLarge,
            quota,
        });
    }
    let mut locked = cache.lock().unwrap();
    let result = finish_in_action(
        cache,
        &mut locked,
        request,
        list,
        (page, quota),
        describe,
        retain,
    );
    request.check(quota)?;
    result
}

fn finish_in_action<K, L, D, T>(
    cache: &SharedCache<K, L, D>,
    locked: &mut CacheMap<K, L, D>,
    request: &SummaryRequest<'_>,
    list: &ListObservations,
    (page, quota): (Page<T>, RateLimitStatus),
    describe: impl Fn(&T) -> (ReviewTarget, bool),
    retain: impl Fn(&mut CacheMap<K, L, D>),
) -> Result<SummaryPage<T>, CacheFailure>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    L: Send + 'static,
    D: Send + 'static,
{
    request.matches_metadata(list, quota)?;
    list.validate_start()
        .map_err(|e| CacheFailure::ineligible(e, quota))?;
    retain(locked);
    let mut items = Vec::with_capacity(page.items.len());
    let mut receipts = Vec::with_capacity(page.items.len());
    let mut targets = Vec::with_capacity(page.items.len());
    let mut omitted = 0;
    for value in page.items {
        let (target, complete) = describe(&value);
        match row_in_action(locked, request, list, &target, complete, quota) {
            Ok(receipt) => {
                items.push(value);
                receipts.push(receipt);
                targets.push(target);
            }
            Err(CacheFailure {
                cause: CacheError::Ineligible(_),
                ..
            }) => omitted += 1,
            Err(error) => return Err(error),
        }
    }
    retain(locked);
    request.check_metadata(quota)?;
    Ok(SummaryPage {
        page: Page {
            items,
            next_cursor: page.next_cursor,
        },
        receipts,
        omitted,
        quota,
        delivery: page_delivery(cache, request.connection, list, targets),
    })
}

/// EVERY reuse also needs the original admitted authority/connection and its
/// existing TTL. No payload or page is stored by this module.
pub(crate) fn can_serve<K: Clone + Eq + Hash, L, D>(
    cache: &SharedCache<K, L, D>,
    request: &SummaryRequest<'_>,
    receipt: &SummaryReceipt,
    max_age: Duration,
) -> Result<bool, CacheFailure> {
    let quota = RateLimitStatus::default();
    request.check(quota)?;
    if !receipt
        .list
        .matches(request.connection, request.repository, request.kind)
    {
        return Err(CacheFailure::ineligible(Ineligible::DifferentSlot, quota));
    }
    let cache = cache.lock().unwrap();
    let eligible = receipt.fetched_at.elapsed() < max_age
        && receipt.list.validate(request.connection).is_ok()
        && matches!(cache.get(&CacheKey::Qualified(Box::new(receipt.key.clone()))),
            Some(CacheSlot::Qualified(slot)) if slot.observations.can_serve(&receipt.observation, request.connection));
    request.check(quota)?;
    Ok(eligible)
}

/// Capture the caller's owned provider query (including filter/search/cursor)
/// and the admitted project before dispatching. Request ordering in the existing
/// pager stays with its owner; this adds no query DTO or retained page store.
pub(crate) async fn read<K, L, D, Q, T, F, Fut>(
    cache: &SharedCache<K, L, D>,
    request: &SummaryRequest<'_>,
    query: Q,
    provider: F,
    describe: impl Fn(&T) -> (ReviewTarget, bool),
    retain: impl Fn(&mut CacheMap<K, L, D>),
) -> Result<SummaryPage<T>, CacheFailure>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    L: Send + 'static,
    D: Send + 'static,
    F: FnOnce(Q) -> Fut,
    Fut: Future<Output = ProviderRead<Page<T>>>,
{
    let list = begin(request)?;
    let outcome = provider(query).await;
    finish(cache, request, &list, outcome, describe, retain)
}

/// The opaque provider target owns the exact admitted query/cursor. No copied
/// query schema, fake resource number, or raw error can authorize invalidation.
pub(crate) struct ManagedSummaryRequest<'a> {
    pub(crate) request: SummaryRequest<'a>,
    pub(crate) target: &'a RepositoryCollectionTarget,
    pub(crate) eligibility: &'a RepositoryReadEligibility,
    pub(crate) with_authority: &'a CacheAdmissionGuard<'a>,
    pub(crate) denials: &'a CacheDenialObserver<'a>,
}

impl ManagedSummaryRequest<'_> {
    fn matches_target(&self, quota: RateLimitStatus) -> Result<(), CacheFailure> {
        self.request.check(quota)?;
        if self.request.repository != self.target.repository()
            || self.request.kind != self.target.kind()
        {
            return Err(CacheFailure {
                cause: CacheError::Credential(RepositoryCredentialError::BoundaryMismatch),
                quota,
            });
        }
        Ok(())
    }

    fn current<T: Send>(
        &self,
        quota: RateLimitStatus,
        action: impl FnOnce() -> Result<T, CacheFailure> + Send,
    ) -> Result<T, CacheFailure> {
        self.matches_target(quota)?;
        let mut action = Some(action);
        let mut output = None;
        (self.with_authority)(&mut || {
            self.eligibility.with_current(&mut || {
                output = Some(action
                    .take()
                    .ok_or(RepositoryCredentialError::Indeterminate)?(
                ));
                Ok(())
            })
        })
        .map_err(|error| CacheFailure {
            cause: CacheError::Credential(error),
            quota,
        })?;
        output.ok_or_else(|| CacheFailure {
            cause: CacheError::Credential(RepositoryCredentialError::Indeterminate),
            quota,
        })?
    }
}

pub(crate) async fn read_managed<K, L, D, T, F, Fut>(
    cache: &SharedCache<K, L, D>,
    managed: &ManagedSummaryRequest<'_>,
    provider: F,
    describe: impl Fn(&T) -> (ReviewTarget, bool) + Send,
    retain: impl Fn(&mut CacheMap<K, L, D>) + Send,
) -> Result<SummaryPage<T>, CacheFailure>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    L: Send + 'static,
    D: Send + 'static,
    T: Send,
    F: FnOnce() -> Fut,
    Fut: Future<Output = RepositoryProviderRead<Page<T>>>,
{
    let request = &managed.request;
    let list = managed.current(RateLimitStatus::default(), || {
        request
            .connection
            .project(request.repository.clone())
            .begin_list(request.kind)
            .map_err(|e| CacheFailure::ineligible(e, RateLimitStatus::default()))
    })?;
    let (outcome, quota, attribution) = provider().await.into_parts();
    let page = match outcome {
        Err(error) => {
            // Error/observed quota survive even when the original caller,
            // connection, or response evidence no longer permits application.
            if managed.matches_target(quota).is_ok() {
                let _ = (managed.with_authority)(&mut || {
                    managed.eligibility.with_collection_response(
                        &attribution,
                        managed.target,
                        &mut |d| {
                            use intent_sourcecontrol::error::ProviderFailureKind as P;
                            use RepositoryResponseDisposition as R;
                            let kind = match d {
                                R::AcceptedCredentialRejection => Some(P::CredentialRejected),
                                R::CurrentProjectDenial => Some(P::ProjectDenied),
                                R::CurrentResourceDenial => Some(P::ResourceDenied),
                                R::NoDenial => None,
                                R::NotApplied | R::Unattributed | R::Indeterminate => return Ok(()),
                            };
                            if list.failure_kind(kind).is_ok() {
                                observe_denial(
                                    d,
                                    request.connection,
                                    request.repository,
                                    CacheDenialCoverage::List(request.kind),
                                    Some(managed.denials),
                                );
                            }
                            Ok(())
                        },
                    )
                });
            }
            return Err(CacheFailure {
                cause: CacheError::Provider(error),
                quota,
            });
        }
        Ok(page) => page,
    };
    managed.matches_target(quota)?;
    if page.items.len() > usize::from(crate::github_ops::clamp_limit(Some(i64::MAX))) {
        return Err(CacheFailure {
            cause: CacheError::PageTooLarge,
            quota,
        });
    }
    let mut locked = cache.lock().unwrap();
    let slots = &mut *locked;
    let mut page = Some(page);
    let mut describe = Some(describe);
    let mut retain = Some(retain);
    let mut output = None;
    (managed.with_authority)(&mut || {
        managed
            .eligibility
            .with_collection_response(&attribution, managed.target, &mut |d| {
                if d == RepositoryResponseDisposition::NoDenial {
                    Ok(())
                } else {
                    Err(RepositoryCredentialError::BoundaryMismatch)
                }
            })?;
        managed.eligibility.with_current(&mut || {
            output = Some(finish_in_action(
                cache,
                slots,
                request,
                &list,
                (
                    page.take()
                        .ok_or(RepositoryCredentialError::Indeterminate)?,
                    quota,
                ),
                describe
                    .take()
                    .ok_or(RepositoryCredentialError::Indeterminate)?,
                retain
                    .take()
                    .ok_or(RepositoryCredentialError::Indeterminate)?,
            ));
            Ok(())
        })
    })
    .map_err(|error| CacheFailure {
        cause: CacheError::Credential(error),
        quota,
    })?;
    output.ok_or_else(|| CacheFailure {
        cause: CacheError::Credential(RepositoryCredentialError::Indeterminate),
        quota,
    })?
}

#[cfg(test)]
mod tests;
