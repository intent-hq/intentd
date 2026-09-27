//! Qualified reads in the existing PR and issue maps. No additional payload cache.
//!
//! The caller directory supplies a captured canonical target, its server-owned
//! connection lifetime, a bounded synchronous admission check and request-scoped
//! provider reads. Admission must check the captured caller/authority, not just
//! credential presence. Provider closures return the actual typed result and
//! connection quota evidence, before any wire-error mapping. They must use that
//! same admitted target; these storage primitives do not resolve a workspace.
//!
//! Caller/RPC and monitor scheduler integration is deliberately separate.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use intent_core::{RepositoryResourceKind, ReviewTarget};
use intent_sourcecontrol::{RateLimitStatus, ReviewDetails, ReviewObservation};

use crate::observation_adapter::{
    complete_snapshot, ConnectionObservations, ObservationReceipt, ObservationTicket, QualifiedKey,
    ReviewObservations,
};
use crate::observation_policy::{Coverage, Ineligible};

use super::{
    retain_pr_cache, PrCache, PrFingerprint, PrKey, PrReadPolicy, PR_MONITOR_MAX_CHEAP_AGE,
    PR_MONITOR_MAX_CHEAP_POLLS,
};

pub(crate) mod summaries;

/// The legacy GitHub boundary never aliases an admitted qualified resource.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum CacheKey<K> {
    Legacy(K),
    Qualified(Box<QualifiedKey>),
}

/// A slot holds exactly one payload representation in the existing cache map.
#[derive(Debug)]
pub(crate) enum CacheSlot<L, T> {
    Legacy(L),
    Qualified(Box<QualifiedSlot<T>>),
}

impl<L: Default, T> Default for CacheSlot<L, T> {
    fn default() -> Self {
        Self::Legacy(L::default())
    }
}

impl<L, T> CacheSlot<L, T> {
    pub(crate) fn legacy(&self) -> Option<&L> {
        match self {
            Self::Legacy(value) => Some(value),
            Self::Qualified(_) => None,
        }
    }

    pub(crate) fn legacy_mut(&mut self) -> Option<&mut L> {
        match self {
            Self::Legacy(value) => Some(value),
            Self::Qualified(_) => None,
        }
    }
}

pub(crate) type CacheMap<K, L, T> = HashMap<CacheKey<K>, CacheSlot<L, T>>;
type SharedCache<K, L, T> = Arc<Mutex<CacheMap<K, L, T>>>;
pub(crate) type ProviderRead<T> = (intent_sourcecontrol::Result<T>, RateLimitStatus);

pub(crate) struct CacheRequest<'a> {
    pub(crate) connection: &'a ConnectionObservations,
    pub(crate) target: &'a ReviewTarget,
    pub(crate) revalidate: &'a (dyn Fn() -> intent_core::Result<()> + Sync),
}

impl CacheRequest<'_> {
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

    fn key<K>(&self) -> CacheKey<K> {
        CacheKey::Qualified(Box::new(self.connection.key(self.target.clone())))
    }
}

#[derive(Debug)]
pub(crate) enum CacheError {
    Admission(intent_core::Error),
    Ineligible(Ineligible),
    Provider(intent_sourcecontrol::Error),
    PageTooLarge,
}

/// Quota evidence survives rejection of a late payload as well as provider errors.
#[derive(Debug)]
pub(crate) struct CacheFailure {
    pub(crate) cause: CacheError,
    pub(crate) quota: RateLimitStatus,
}

impl CacheFailure {
    pub(crate) fn ineligible(reason: Ineligible, quota: RateLimitStatus) -> Self {
        Self {
            cause: CacheError::Ineligible(reason),
            quota,
        }
    }
}

#[derive(Debug)]
pub(crate) struct CacheRead<T> {
    pub(crate) value: T,
    pub(crate) quota: RateLimitStatus,
    pub(crate) fetched: bool,
}

#[derive(Debug)]
struct Payload<T> {
    value: T,
    quota: RateLimitStatus,
    receipt: ObservationReceipt,
    freshness: Freshness,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Freshness {
    fetched_at: Instant,
    refreshed_at: Instant,
    cheap_polls: u32,
}

impl Freshness {
    pub(crate) fn full() -> Self {
        let now = Instant::now();
        Self {
            fetched_at: now,
            refreshed_at: now,
            cheap_polls: 0,
        }
    }
}

#[derive(Debug)]
pub(crate) struct QualifiedSlot<T> {
    observations: ReviewObservations,
    payload: Option<Payload<T>>,
    created_at: Instant,
    /// A newer partial/failed full read must retry fields even while an older
    /// complete payload is still within its original Serve window.
    reuse_allowed: bool,
    /// Set only by the monitor owner; unrelated legacy sweeps preserve this bit.
    pub(crate) monitored: bool,
}

impl<T> QualifiedSlot<T> {
    fn age_anchor(&self) -> Instant {
        self.payload
            .as_ref()
            .map_or(self.created_at, |p| p.freshness.fetched_at)
    }
}

impl<T> Drop for QualifiedSlot<T> {
    fn drop(&mut self) {
        self.observations.evicted();
    }
}

pub(crate) enum Started<T> {
    Hit(CacheRead<T>),
    Read {
        ticket: ObservationTicket,
        previous: Option<(T, Freshness)>,
    },
}

/// Insert request-start metadata into the SAME bounded map as response payloads.
/// Evicting a pending slot drops its right to write; there is no tombstone map.
pub(crate) fn start<K: Clone + Eq + Hash, L, T: Clone>(
    cache: &SharedCache<K, L, T>,
    request: &CacheRequest<'_>,
    max_age: Duration,
    retain: impl Fn(&mut CacheMap<K, L, T>),
) -> Result<Started<T>, CacheFailure> {
    let quota = RateLimitStatus::default();
    request.check(quota)?;
    let key = request.key();
    let mut cache = cache.lock().unwrap();
    // Expire the old lifetime before issuing a new ticket for the refresh.
    // Otherwise the post-insertion pass could evict that very ticket.
    retain(&mut cache);
    let slot = cache.entry(key).or_insert_with(|| new_slot(request));
    let CacheSlot::Qualified(slot) = slot else {
        unreachable!("qualified cache key")
    };
    if !slot.observations.belongs_to(request.connection) {
        **slot = new_qualified_slot(request);
    }
    let previous = slot
        .payload
        .as_ref()
        .filter(|p| slot.observations.can_serve(&p.receipt, request.connection));
    if let Some(payload) = previous.filter(|p| p.freshness.refreshed_at.elapsed() < max_age) {
        request.check(payload.quota)?;
        return Ok(Started::Hit(CacheRead {
            value: payload.value.clone(),
            quota: payload.quota,
            fetched: false,
        }));
    }
    let previous = previous
        .filter(|_| slot.reuse_allowed)
        .map(|p| (p.value.clone(), p.freshness));
    let ticket = slot
        .observations
        .begin(Coverage::Detail)
        .map_err(|e| CacheFailure::ineligible(e, quota))?;
    retain(&mut cache);
    request.check(quota)?;
    Ok(Started::Read { ticket, previous })
}

fn new_slot<L, T>(request: &CacheRequest<'_>) -> CacheSlot<L, T> {
    CacheSlot::Qualified(Box::new(new_qualified_slot(request)))
}

fn new_qualified_slot<T>(request: &CacheRequest<'_>) -> QualifiedSlot<T> {
    QualifiedSlot {
        observations: request
            .connection
            .project(request.target.repository.clone())
            .slot(request.target.kind, request.target.number),
        payload: None,
        created_at: Instant::now(),
        reuse_allowed: false,
        monitored: false,
    }
}

/// Apply errors before any lossy mapping. Both cache kinds share connection and
/// project lifetimes, so a denial observed by an issue also fences review data.
pub(crate) fn finish<K: Clone + Eq + Hash, L, T: Clone>(
    cache: &SharedCache<K, L, T>,
    request: &CacheRequest<'_>,
    ticket: ObservationTicket,
    outcome: ProviderRead<T>,
    complete: impl FnOnce(&T) -> bool,
    freshness: Freshness,
    retain: impl FnOnce(&mut CacheMap<K, L, T>),
) -> Result<CacheRead<T>, CacheFailure> {
    let (outcome, quota) = outcome;
    request.check(quota)?;
    let mut cache = cache.lock().unwrap();
    let Some(CacheSlot::Qualified(slot)) = cache.get_mut(&request.key()) else {
        return Err(CacheFailure::ineligible(Ineligible::DifferentSlot, quota));
    };
    let value = match outcome {
        Ok(value) => value,
        Err(error) => {
            let (error, quota) = slot
                .observations
                .failure(ticket, error, quota)
                .map_err(|e| CacheFailure::ineligible(e, quota))?;
            slot.reuse_allowed = false;
            return Err(CacheFailure {
                cause: CacheError::Provider(error),
                quota,
            });
        }
    };
    slot.observations
        .validate(&ticket)
        .map_err(|e| CacheFailure::ineligible(e, quota))?;
    // A cheap poll captured its previous fields before the provider await.
    // A failed full read may have disabled reuse since then. Check under the
    // map lock so that captured fields cannot bypass the current reuse fence.
    if freshness.cheap_polls > 0 && !slot.reuse_allowed {
        return Err(CacheFailure::ineligible(
            Ineligible::OlderObservation,
            quota,
        ));
    }
    request.check(quota)?;
    slot.reuse_allowed = complete(&value);
    if slot.reuse_allowed {
        let receipt = slot
            .observations
            .primary_success(ticket)
            .map_err(|e| CacheFailure::ineligible(e, quota))?;
        slot.payload = Some(Payload {
            value: value.clone(),
            quota,
            receipt,
            freshness,
        });
    } else {
        slot.observations
            .partial_success(&ticket)
            .map_err(|e| CacheFailure::ineligible(e, quota))?;
    }
    // Partial results retain their exact fields but never refresh old cache data.
    retain(&mut cache);
    Ok(CacheRead {
        value,
        quota,
        fetched: true,
    })
}

/// Existing limits apply across legacy AND qualified rows, including in-flight
/// metadata. A monitored review remains exempt; issues pass no exemptions.
pub(crate) fn retain<K: Clone + Eq + Hash, L, T>(
    cache: &mut CacheMap<K, L, T>,
    legacy_anchor: impl Fn(&L) -> Option<Instant>,
    legacy_monitored: impl Fn(&K) -> bool,
    limits: (Duration, usize),
    now: Instant,
) {
    let anchor = |slot: &CacheSlot<L, T>| match slot {
        CacheSlot::Legacy(row) => legacy_anchor(row),
        CacheSlot::Qualified(row) => Some(row.age_anchor()),
    };
    let monitored = |key: &CacheKey<K>, slot: &CacheSlot<L, T>| match (key, slot) {
        (CacheKey::Legacy(key), _) => legacy_monitored(key),
        (_, CacheSlot::Qualified(row)) => row.monitored,
        _ => false,
    };
    cache.retain(|key, slot| {
        monitored(key, slot)
            || anchor(slot).is_some_and(|at| now.saturating_duration_since(at) < limits.0)
    });
    let mut unmonitored: Vec<_> = cache
        .iter()
        .filter(|(k, s)| !monitored(k, s))
        .map(|(k, s)| (anchor(s).unwrap_or(now), k.clone()))
        .collect();
    let excess = unmonitored.len().saturating_sub(limits.1);
    if excess > 0 {
        unmonitored.sort_by_key(|(at, _)| *at);
        for (_, key) in unmonitored.into_iter().take(excess) {
            cache.remove(&key);
        }
    }
}

/// Qualified review read with the legacy cheap-poll bounds and fingerprint.
/// The directory binds both closures to the same provider/instance/project and
/// forwards returned quota to its per-connection backoff owner. Neither closure
/// is retained by the cache. No legacy error fallback can hide a typed denial.
pub(crate) async fn read_review<P, PF, F, FF>(
    cache: &PrCache,
    request: &CacheRequest<'_>,
    policy: PrReadPolicy,
    monitored: &HashSet<PrKey>,
    primary: P,
    full: F,
) -> Result<CacheRead<ReviewObservation>, CacheFailure>
where
    P: FnOnce() -> PF,
    PF: Future<Output = ProviderRead<ReviewDetails>>,
    F: FnOnce() -> FF,
    FF: Future<Output = ProviderRead<ReviewObservation>>,
{
    if request.target.kind == RepositoryResourceKind::Issue {
        return Err(CacheFailure::ineligible(
            Ineligible::DifferentSlot,
            RateLimitStatus::default(),
        ));
    }
    let max_age = match policy {
        PrReadPolicy::Serve { max_age } => max_age,
        PrReadPolicy::Poll => Duration::ZERO,
    };
    let (ticket, previous) = match start(cache, request, max_age, |c| {
        retain_pr_cache(c, monitored, Instant::now());
    })? {
        Started::Hit(hit) => return Ok(hit),
        Started::Read { ticket, previous } => (ticket, previous),
    };
    let keep = |c: &mut _| retain_pr_cache(c, monitored, Instant::now());
    let mut observed_quota = RateLimitStatus::default();
    if matches!(policy, PrReadPolicy::Poll) {
        request.check(RateLimitStatus::default())?;
        let (details, quota) = primary().await;
        observed_quota = quota;
        request.check(quota)?;
        let details = match details {
            Ok(details) => details,
            Err(error) => {
                return finish(
                    cache,
                    request,
                    ticket,
                    (Err(error), quota),
                    complete_snapshot,
                    Freshness::full(),
                    keep,
                )
            }
        };
        {
            let cache = cache.lock().unwrap();
            let Some(CacheSlot::Qualified(slot)) = cache.get(&request.key()) else {
                return Err(CacheFailure::ineligible(Ineligible::DifferentSlot, quota));
            };
            slot.observations
                .validate(&ticket)
                .map_err(|e| CacheFailure::ineligible(e, quota))?;
        }
        if let Some((mut old, mut freshness)) = previous {
            let fingerprint = PrFingerprint::of(&details.review);
            if fingerprint.detects_changes()
                && fingerprint == PrFingerprint::of(&old.details.review)
                && complete_snapshot(&old)
                && freshness.cheap_polls < PR_MONITOR_MAX_CHEAP_POLLS
                && freshness.fetched_at.elapsed() < PR_MONITOR_MAX_CHEAP_AGE
            {
                old.details = details;
                freshness.cheap_polls += 1;
                freshness.refreshed_at = Instant::now();
                return finish(
                    cache,
                    request,
                    ticket,
                    (Ok(old), quota),
                    complete_snapshot,
                    freshness,
                    keep,
                );
            }
        }
    }
    request.check(observed_quota)?;
    let outcome = full().await;
    finish(
        cache,
        request,
        ticket,
        outcome,
        complete_snapshot,
        Freshness::full(),
        keep,
    )
}

#[cfg(test)]
mod tests;
