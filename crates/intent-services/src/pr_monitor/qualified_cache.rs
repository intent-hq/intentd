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
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use crate::repository_credentials::read::{
    RepositoryProviderRead, RepositoryResponseAttribution, RepositoryResponseDisposition,
};
use crate::repository_credentials::{RepositoryCredentialError, Result as CredentialResult};
use crate::source_control_auth_ops::repository_owner::RepositoryReadEligibility;
use intent_core::{RepositoryResourceKind, ReviewTarget};
use intent_sourcecontrol::{
    error::ProviderFailureKind, RateLimitStatus, ReviewDetails, ReviewObservation,
};

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

/// The original caller owner supplies this short synchronous fence. It must not
/// await, perform I/O or reacquire the cache. This is not a permission producer.
pub(crate) type CacheAdmissionAction<'a> = dyn FnMut() -> CredentialResult<()> + Send + 'a;
pub(crate) type CacheAdmissionGuard<'a> =
    dyn Fn(&mut CacheAdmissionAction<'_>) -> CredentialResult<()> + Sync + 'a;

/// Read-only original-caller checks stay in `request`; credential eligibility is
/// separate so a rejection does not erase its own provider error. The owner must
/// bind these observations to the SAME admitted execution/connection lifetime.
/// Final delivery still needs its fresh original-caller fence outside the cache.
pub(crate) struct ManagedCacheRequest<'a> {
    pub(crate) request: CacheRequest<'a>,
    pub(crate) eligibility: &'a RepositoryReadEligibility,
    pub(crate) with_authority: &'a CacheAdmissionGuard<'a>,
}

#[derive(Clone, Copy)]
pub(crate) enum DetailAccess<'r, 'a> {
    Legacy(&'r CacheRequest<'a>),
    Managed(&'r ManagedCacheRequest<'a>),
}

pub(crate) struct DetailOutcome<T> {
    value: intent_sourcecontrol::Result<T>,
    quota: RateLimitStatus,
    attribution: Option<Arc<RepositoryResponseAttribution>>,
}
impl<T> From<ProviderRead<T>> for DetailOutcome<T> {
    fn from((value, quota): ProviderRead<T>) -> Self {
        Self {
            value,
            quota,
            attribution: None,
        }
    }
}
impl<T> From<RepositoryProviderRead<T>> for DetailOutcome<T> {
    fn from(outcome: RepositoryProviderRead<T>) -> Self {
        let (value, quota, attribution) = outcome.into_parts();
        Self {
            value,
            quota,
            attribution: Some(attribution),
        }
    }
}

impl<'r, 'a> DetailAccess<'r, 'a> {
    fn request(self) -> &'r CacheRequest<'a> {
        match self {
            Self::Legacy(r) => r,
            Self::Managed(r) => &r.request,
        }
    }
    fn check(self, quota: RateLimitStatus) -> Result<(), CacheFailure> {
        match self {
            Self::Legacy(r) => r.check(quota),
            Self::Managed(_) => self.with_current(quota, None, || Ok(())),
        }
    }
    // Managed actions already hold the original R and P fences. Do not call
    // either owner back from inside their locks.
    fn check_in_action(self, quota: RateLimitStatus) -> Result<(), CacheFailure> {
        if matches!(self, Self::Legacy(_)) {
            return self.request().check(quota);
        }
        if !self.request().connection.is_active() {
            return Err(CacheFailure::ineligible(Ineligible::RetiredScope, quota));
        }
        Ok(())
    }
    fn response_current<T: Send>(
        self,
        quota: RateLimitStatus,
        attribution: Option<&RepositoryResponseAttribution>,
        action: impl FnOnce() -> Result<T, CacheFailure> + Send,
    ) -> Result<T, CacheFailure> {
        if matches!(self, Self::Managed(_)) && attribution.is_none() {
            return Err(CacheFailure {
                cause: CacheError::Credential(RepositoryCredentialError::BoundaryMismatch),
                quota,
            });
        }
        self.with_current(quota, attribution, action)
    }
    fn with_current<T: Send>(
        self,
        quota: RateLimitStatus,
        attribution: Option<&RepositoryResponseAttribution>,
        action: impl FnOnce() -> Result<T, CacheFailure> + Send,
    ) -> Result<T, CacheFailure> {
        let Self::Managed(r) = self else {
            return action();
        };
        r.request.check(quota)?;
        let mut action = Some(action);
        let mut output = None;
        (r.with_authority)(&mut || {
            if let Some(attribution) = attribution {
                r.eligibility
                    .with_response(attribution, r.request.target, &mut |d| {
                        if d == RepositoryResponseDisposition::NoDenial {
                            Ok(())
                        } else {
                            Err(RepositoryCredentialError::BoundaryMismatch)
                        }
                    })?;
            }
            r.eligibility.with_current(&mut || {
                let action = action
                    .take()
                    .ok_or(RepositoryCredentialError::Indeterminate)?;
                output = Some(action());
                Ok(())
            })
        })
        .map_err(|error| CacheFailure {
            cause: CacheError::Credential(error),
            quota,
        })?;
        output.unwrap_or_else(|| {
            Err(CacheFailure {
                cause: CacheError::Credential(RepositoryCredentialError::Indeterminate),
                quota,
            })
        })
    }
}

#[derive(Debug)]
pub(crate) enum CacheError {
    Admission(intent_core::Error),
    Credential(RepositoryCredentialError),
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
    pub(crate) delivery: CacheDelivery,
}

/// A retained payload still needs its original observation at final transfer.
/// This owns no payload or authority. Cache -> caller -> provider is the same
/// lock order as start/install, so denial and transfer are serialized.
#[derive(Clone)]
pub(crate) struct CacheDelivery(Arc<DeliveryCheck>);
type DeliveryCheck = dyn Fn(&mut CacheAdmissionAction<'_>) -> CredentialResult<()> + Send + Sync;
impl std::fmt::Debug for CacheDelivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CacheDelivery(original observation)")
    }
}
impl CacheDelivery {
    pub(crate) fn with_current(
        &self,
        action: &mut CacheAdmissionAction<'_>,
    ) -> CredentialResult<()> {
        (self.0)(action)
    }
}
enum DeliveryObservation {
    Complete(Arc<ObservationReceipt>),
    Partial(ObservationTicket),
}
fn delivery<K: Eq + Hash + Send + Sync + 'static, L: Send + 'static, T: Send + 'static>(
    cache: Weak<Mutex<CacheMap<K, L, T>>>,
    key: CacheKey<K>,
    connection: ConnectionObservations,
    observation: DeliveryObservation,
) -> CacheDelivery {
    CacheDelivery(Arc::new(move |action| {
        let cache = cache.upgrade().ok_or(RepositoryCredentialError::Retired)?;
        let locked = cache
            .lock()
            .map_err(|_| RepositoryCredentialError::Indeterminate)?;
        let Some(CacheSlot::Qualified(slot)) = locked.get(&key) else {
            return Err(RepositoryCredentialError::Retired);
        };
        let current = slot.observations.belongs_to(&connection)
            && match &observation {
                DeliveryObservation::Complete(receipt) => {
                    slot.observations.can_serve(receipt, &connection)
                }
                DeliveryObservation::Partial(ticket) => slot.observations.validate(ticket).is_ok(),
            };
        if !current {
            return Err(RepositoryCredentialError::Retired);
        }
        action()
    }))
}

#[derive(Debug)]
struct Payload<T> {
    value: T,
    quota: RateLimitStatus,
    receipt: Arc<ObservationReceipt>,
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
pub(crate) fn start<
    K: Clone + Eq + Hash + Send + Sync + 'static,
    L: Send + 'static,
    T: Clone + Send + 'static,
>(
    cache: &SharedCache<K, L, T>,
    request: &CacheRequest<'_>,
    max_age: Duration,
    retain: impl Fn(&mut CacheMap<K, L, T>) + Send,
) -> Result<Started<T>, CacheFailure> {
    start_access(cache, DetailAccess::Legacy(request), max_age, retain)
}

pub(crate) fn start_access<
    K: Clone + Eq + Hash + Send + Sync + 'static,
    L: Send + 'static,
    T: Clone + Send + 'static,
>(
    cache: &SharedCache<K, L, T>,
    access: DetailAccess<'_, '_>,
    max_age: Duration,
    retain: impl Fn(&mut CacheMap<K, L, T>) + Send,
) -> Result<Started<T>, CacheFailure> {
    let request = access.request();
    let quota = RateLimitStatus::default();
    access.check(quota)?;
    let key = request.key();
    let delivery_cache = Arc::downgrade(cache);
    let delivery_key = key.clone();
    let mut locked = cache.lock().unwrap();
    let cache = &mut *locked;
    access.with_current(quota, None, move || {
        // Expire the old lifetime before issuing a new ticket for the refresh.
        // Otherwise the post-insertion pass could evict that very ticket.
        retain(cache);
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
            access.check_in_action(payload.quota)?;
            return Ok(Started::Hit(CacheRead {
                value: payload.value.clone(),
                quota: payload.quota,
                fetched: false,
                delivery: delivery(
                    delivery_cache,
                    delivery_key,
                    request.connection.clone(),
                    DeliveryObservation::Complete(payload.receipt.clone()),
                ),
            }));
        }
        let previous = previous
            .filter(|_| slot.reuse_allowed)
            .map(|p| (p.value.clone(), p.freshness));
        let ticket = slot
            .observations
            .begin(Coverage::Detail)
            .map_err(|e| CacheFailure::ineligible(e, quota))?;
        retain(cache);
        access.check_in_action(quota)?;
        Ok(Started::Read { ticket, previous })
    })
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
pub(crate) fn finish<
    K: Clone + Eq + Hash + Send + Sync + 'static,
    L: Send + 'static,
    T: Clone + Send + 'static,
>(
    cache: &SharedCache<K, L, T>,
    request: &CacheRequest<'_>,
    ticket: ObservationTicket,
    outcome: ProviderRead<T>,
    complete: impl FnOnce(&T) -> bool + Send,
    freshness: Freshness,
    retain: impl FnOnce(&mut CacheMap<K, L, T>) + Send,
) -> Result<CacheRead<T>, CacheFailure> {
    finish_access(
        cache,
        DetailAccess::Legacy(request),
        ticket,
        outcome.into(),
        complete,
        freshness,
        retain,
    )
}

pub(crate) fn finish_access<
    K: Clone + Eq + Hash + Send + Sync + 'static,
    L: Send + 'static,
    T: Clone + Send + 'static,
>(
    cache: &SharedCache<K, L, T>,
    access: DetailAccess<'_, '_>,
    ticket: ObservationTicket,
    outcome: DetailOutcome<T>,
    complete: impl FnOnce(&T) -> bool + Send,
    freshness: Freshness,
    retain: impl FnOnce(&mut CacheMap<K, L, T>) + Send,
) -> Result<CacheRead<T>, CacheFailure> {
    let request = access.request();
    let DetailOutcome {
        value: outcome,
        quota,
        attribution,
    } = outcome;
    if let (DetailAccess::Managed(managed), Err(_)) = (access, &outcome) {
        // The response error is already known. Eligibility/slot refusal must not
        // turn it into a synthetic local error or discard its quota.
        let mut locked = cache.lock().unwrap();
        if let Some(CacheSlot::Qualified(slot)) = locked.get_mut(&request.key()) {
            if slot.observations.belongs_to(request.connection) {
                if let Some(attribution) = &attribution {
                    let mut ticket = Some(ticket);
                    let _ = (managed.with_authority)(&mut || {
                        managed
                            .eligibility
                            .with_response(attribution, request.target, &mut |d| {
                                use RepositoryResponseDisposition as D;
                                let kind = match d {
                                    D::AcceptedCredentialRejection => {
                                        Some(ProviderFailureKind::CredentialRejected)
                                    }
                                    D::CurrentProjectDenial => {
                                        Some(ProviderFailureKind::ProjectDenied)
                                    }
                                    D::CurrentResourceDenial => {
                                        Some(ProviderFailureKind::ResourceDenied)
                                    }
                                    D::NoDenial => None,
                                    D::NotApplied | D::Unattributed | D::Indeterminate => {
                                        return Ok(())
                                    }
                                };
                                let ticket = ticket
                                    .take()
                                    .ok_or(RepositoryCredentialError::Indeterminate)?;
                                if slot.observations.failure_kind(ticket, kind).is_ok() {
                                    slot.reuse_allowed = false;
                                }
                                Ok(())
                            })
                    });
                }
            }
        }
        return Err(CacheFailure {
            cause: CacheError::Provider(outcome.err().unwrap()),
            quota,
        });
    }
    access.check(quota)?;
    let delivery_cache = Arc::downgrade(cache);
    let delivery_key = request.key();
    let mut locked = cache.lock().unwrap();
    let cache = &mut *locked;
    access.response_current(quota, attribution.as_deref(), || {
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
        access.check_in_action(quota)?;
        slot.reuse_allowed = complete(&value);
        let delivery_observation = if slot.reuse_allowed {
            let receipt = Arc::new(
                slot.observations
                    .primary_success(ticket)
                    .map_err(|e| CacheFailure::ineligible(e, quota))?,
            );
            slot.payload = Some(Payload {
                value: value.clone(),
                quota,
                receipt: receipt.clone(),
                freshness,
            });
            DeliveryObservation::Complete(receipt)
        } else {
            slot.observations
                .partial_success(&ticket)
                .map_err(|e| CacheFailure::ineligible(e, quota))?;
            DeliveryObservation::Partial(ticket)
        };
        // Partial results retain their exact fields but never refresh old cache data.
        retain(cache);
        Ok(CacheRead {
            value,
            quota,
            fetched: true,
            delivery: delivery(
                delivery_cache,
                delivery_key,
                request.connection.clone(),
                delivery_observation,
            ),
        })
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
    read_review_access(
        cache,
        DetailAccess::Legacy(request),
        policy,
        monitored,
        || async { primary().await.into() },
        || async { full().await.into() },
    )
    .await
}

/// Each consuming managed provider call contributes its OWN opaque receipt.
/// Eligibility is not network readiness; backoff is enforced by that call.
pub(crate) async fn read_managed_review<P, PF, F, FF>(
    cache: &PrCache,
    request: &ManagedCacheRequest<'_>,
    policy: PrReadPolicy,
    monitored: &HashSet<PrKey>,
    primary: P,
    full: F,
) -> Result<CacheRead<ReviewObservation>, CacheFailure>
where
    P: FnOnce() -> PF,
    PF: Future<Output = RepositoryProviderRead<ReviewDetails>>,
    F: FnOnce() -> FF,
    FF: Future<Output = RepositoryProviderRead<ReviewObservation>>,
{
    read_review_access(
        cache,
        DetailAccess::Managed(request),
        policy,
        monitored,
        || async { primary().await.into() },
        || async { full().await.into() },
    )
    .await
}

async fn read_review_access<P, PF, F, FF>(
    cache: &PrCache,
    access: DetailAccess<'_, '_>,
    policy: PrReadPolicy,
    monitored: &HashSet<PrKey>,
    primary: P,
    full: F,
) -> Result<CacheRead<ReviewObservation>, CacheFailure>
where
    P: FnOnce() -> PF,
    PF: Future<Output = DetailOutcome<ReviewDetails>>,
    F: FnOnce() -> FF,
    FF: Future<Output = DetailOutcome<ReviewObservation>>,
{
    let request = access.request();
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
    let (ticket, previous) = match start_access(cache, access, max_age, |c| {
        retain_pr_cache(c, monitored, Instant::now());
    })? {
        Started::Hit(hit) => return Ok(hit),
        Started::Read { ticket, previous } => (ticket, previous),
    };
    let keep = |c: &mut _| retain_pr_cache(c, monitored, Instant::now());
    let mut observed_quota = RateLimitStatus::default();
    if matches!(policy, PrReadPolicy::Poll) {
        access.check(RateLimitStatus::default())?;
        let DetailOutcome {
            value: details,
            quota,
            attribution,
        } = primary().await;
        observed_quota = quota;
        if matches!(access, DetailAccess::Legacy(_)) {
            request.check(quota)?;
        }
        let details = match details {
            Ok(details) => details,
            Err(error) => {
                return finish_access(
                    cache,
                    access,
                    ticket,
                    DetailOutcome {
                        value: Err(error),
                        quota,
                        attribution,
                    },
                    complete_snapshot,
                    Freshness::full(),
                    keep,
                )
            }
        };
        access.response_current(quota, attribution.as_deref(), || Ok(()))?;
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
                return finish_access(
                    cache,
                    access,
                    ticket,
                    DetailOutcome {
                        value: Ok(old),
                        quota,
                        attribution,
                    },
                    complete_snapshot,
                    freshness,
                    keep,
                );
            }
        }
    }
    access.check(observed_quota)?;
    let outcome = full().await;
    finish_access(
        cache,
        access,
        ticket,
        outcome,
        complete_snapshot,
        Freshness::full(),
        keep,
    )
}

#[cfg(test)]
mod tests;
