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

use intent_core::{RepositoryResourceKind, RepositoryTarget, ReviewTarget};
use intent_sourcecontrol::{Page, RateLimitStatus};

use super::{
    new_qualified_slot, new_slot, CacheError, CacheFailure, CacheKey, CacheMap, CacheRequest,
    CacheSlot, ProviderRead, SharedCache,
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
        if !self.connection.is_active() {
            return Err(CacheFailure::ineligible(Ineligible::RetiredScope, quota));
        }
        Ok(())
    }

    fn matches(&self, list: &ListObservations, quota: RateLimitStatus) -> Result<(), CacheFailure> {
        self.check(quota)?;
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
    if let Err(error) = request.matches(list, quota) {
        return error;
    }
    match list.failure(error) {
        Ok(error) => CacheFailure {
            cause: CacheError::Provider(error),
            quota,
        },
        Err(error) => CacheFailure::ineligible(error, quota),
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
    request.check(quota)?;
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
pub(crate) fn finish<K: Clone + Eq + Hash, L, D, T>(
    cache: &SharedCache<K, L, D>,
    request: &SummaryRequest<'_>,
    list: &ListObservations,
    outcome: ProviderRead<Page<T>>,
    describe: impl Fn(&T) -> (ReviewTarget, bool),
    retain: impl Fn(&mut CacheMap<K, L, D>),
) -> Result<SummaryPage<T>, CacheFailure> {
    let (outcome, quota) = outcome;
    request.matches(list, quota)?;
    let page = outcome.map_err(|error| failure(request, list, error, quota))?;
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
    let mut cache = cache.lock().unwrap();
    retain(&mut cache);
    let mut items = Vec::with_capacity(page.items.len());
    let mut receipts = Vec::with_capacity(page.items.len());
    let mut omitted = 0;
    for value in page.items {
        let (target, complete) = describe(&value);
        match row(&mut cache, request, list, &target, complete, quota) {
            Ok(receipt) => {
                items.push(value);
                receipts.push(receipt);
            }
            Err(CacheFailure {
                cause: CacheError::Ineligible(_),
                ..
            }) => omitted += 1,
            Err(error) => return Err(error),
        }
    }
    retain(&mut cache);
    request.check(quota)?;
    Ok(SummaryPage {
        page: Page {
            items,
            next_cursor: page.next_cursor,
        },
        receipts,
        omitted,
        quota,
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
pub(crate) async fn read<K: Clone + Eq + Hash, L, D, Q, T, F, Fut>(
    cache: &SharedCache<K, L, D>,
    request: &SummaryRequest<'_>,
    query: Q,
    provider: F,
    describe: impl Fn(&T) -> (ReviewTarget, bool),
    retain: impl Fn(&mut CacheMap<K, L, D>),
) -> Result<SummaryPage<T>, CacheFailure>
where
    F: FnOnce(Q) -> Fut,
    Fut: Future<Output = ProviderRead<Page<T>>>,
{
    let list = begin(request)?;
    let outcome = provider(query).await;
    finish(cache, request, &list, outcome, describe, retain)
}

#[cfg(test)]
mod tests;
