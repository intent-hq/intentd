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

use std::any::Any;
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
use intent_core::{RepositoryResourceKind, RepositoryTarget, ReviewTarget};
use intent_sourcecontrol::{
    error::ProviderFailureKind, RateLimitStatus, ReviewDetails, ReviewObservation,
};

use crate::observation_adapter::{
    complete_snapshot, ConnectionObservations, ListObservations, ObservationReceipt,
    ObservationTicket, QualifiedKey, ReviewObservations,
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

/// Sanitized notification only. It cannot replay a denial into a successor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AcceptedCacheDenial {
    pub(crate) repository: RepositoryTarget,
    pub(crate) coverage: CacheDenialCoverage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CacheDenialCoverage {
    Connection,
    Project,
    Resource(ReviewTarget),
    List(RepositoryResourceKind),
}

/// Borrowed only while the original R/P response guards are held. R selects
/// current matching physical-caller/account recipients before entering them.
/// The callback may update these metadata handles but cannot acquire a cache,
/// R lock, do I/O, await, or retain this capability beyond the callback.
pub(crate) struct GuardedCacheDenial<'a> {
    original: &'a ConnectionObservations,
    accepted: &'a AcceptedCacheDenial,
}

impl GuardedCacheDenial<'_> {
    pub(crate) fn notification(&self) -> AcceptedCacheDenial {
        self.accepted.clone()
    }

    pub(crate) fn apply_to(&self, recipient: &ConnectionObservations) -> bool {
        if !recipient.same_connection(self.original) {
            return false;
        }
        match &self.accepted.coverage {
            CacheDenialCoverage::Connection => recipient.deny_connection(),
            CacheDenialCoverage::Project => recipient.deny_project(&self.accepted.repository),
            CacheDenialCoverage::Resource(target) => recipient.deny_resource(target),
            CacheDenialCoverage::List(kind) => {
                recipient.deny_list(&self.accepted.repository, *kind);
            }
        }
        true
    }
}

pub(crate) type CacheDenialObserver<'a> = dyn Fn(&GuardedCacheDenial<'_>) + Sync + 'a;

fn observe_denial(
    disposition: RepositoryResponseDisposition,
    connection: &ConnectionObservations,
    repository: &RepositoryTarget,
    addressed: CacheDenialCoverage,
    observer: Option<&CacheDenialObserver<'_>>,
) {
    use RepositoryResponseDisposition as D;
    let coverage = match disposition {
        D::AcceptedCredentialRejection => CacheDenialCoverage::Connection,
        D::CurrentProjectDenial => CacheDenialCoverage::Project,
        D::CurrentResourceDenial => addressed,
        D::NoDenial | D::NotApplied | D::Unattributed | D::Indeterminate => return,
    };
    if let Some(observer) = observer {
        let accepted = AcceptedCacheDenial {
            repository: repository.clone(),
            coverage,
        };
        observer(&GuardedCacheDenial {
            original: connection,
            accepted: &accepted,
        });
    }
}

#[derive(Clone, Copy)]
pub(crate) enum DetailAccess<'r, 'a> {
    Legacy(&'r CacheRequest<'a>),
    Managed(&'r ManagedCacheRequest<'a>),
    Observed(&'r ManagedCacheRequest<'a>, &'r CacheDenialObserver<'a>),
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
    fn managed(self) -> Option<&'r ManagedCacheRequest<'a>> {
        match self {
            Self::Legacy(_) => None,
            Self::Managed(r) | Self::Observed(r, _) => Some(r),
        }
    }
    fn observer(self) -> Option<&'r CacheDenialObserver<'a>> {
        match self {
            Self::Observed(_, observer) => Some(observer),
            _ => None,
        }
    }
    fn request(self) -> &'r CacheRequest<'a> {
        match self {
            Self::Legacy(r) => r,
            Self::Managed(r) | Self::Observed(r, _) => &r.request,
        }
    }
    fn check(self, quota: RateLimitStatus) -> Result<(), CacheFailure> {
        match self {
            Self::Legacy(r) => r.check(quota),
            Self::Managed(_) | Self::Observed(_, _) => self.with_current(quota, None, || Ok(())),
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
        if self.managed().is_some() && attribution.is_none() {
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
        let Some(r) = self.managed() else {
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
pub(crate) struct CacheDelivery {
    cache: usize,
    receipt: Arc<dyn Any + Send + Sync>,
    check: Arc<DeliveryCheck>,
    controls: Vec<Arc<dyn intent_core::repository_request::RepositoryMonitorEventSource>>,
}
pub(crate) type CacheRevalidationCheck<'a> = dyn Fn() -> CredentialResult<()> + Send + Sync + 'a;
pub(crate) type CacheRevalidationAction<'a> =
    dyn FnMut(&CacheRevalidationCheck<'_>) -> CredentialResult<()> + Send + 'a;
type DeliveryCheck = dyn Fn(&[&CacheDelivery], &mut CacheRevalidationAction<'_>) -> CredentialResult<()>
    + Send
    + Sync;
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
        self.with_revalidation(&mut |check| {
            check()?;
            action()
        })
    }

    /// Keep the original map locked and supply a borrowed metadata-only check.
    /// The caller repeats it INSIDE its final R/P fence just before transfer.
    /// This check never reacquires a cache or any caller/provider owner lock.
    pub(crate) fn with_revalidation(
        &self,
        action: &mut CacheRevalidationAction<'_>,
    ) -> CredentialResult<()> {
        Self::with_controls(&self.controls.iter().collect::<Vec<_>>(), &mut || {
            (self.check)(&[self], action)
        })
    }

    /// A request may retain several reads (and repeated aliases) from the same
    /// map. Lock each original map once, in stable order, and keep every receipt
    /// current through the caller/provider transfer rather than relocking it.
    pub(crate) fn with_all_current(
        deliveries: &[&Self],
        action: &mut CacheAdmissionAction<'_>,
    ) -> CredentialResult<()> {
        Self::with_all_revalidation(deliveries, &mut |check| {
            check()?;
            action()
        })
    }

    pub(crate) fn with_all_revalidation(
        deliveries: &[&Self],
        action: &mut CacheRevalidationAction<'_>,
    ) -> CredentialResult<()> {
        let mut ordered = deliveries.to_vec();
        ordered.sort_unstable_by_key(|delivery| delivery.cache);
        let mut controls = deliveries
            .iter()
            .flat_map(|d| d.controls.iter())
            .collect::<Vec<_>>();
        controls.sort_unstable_by_key(|c| Arc::as_ptr(c).cast::<()>() as usize);
        controls.dedup_by(|a, b| Arc::ptr_eq(a, b));
        Self::with_controls(&controls, &mut || Self::with_groups(&ordered, &[], action))
    }

    /// Retain the requesting native lifetime alongside the original observation.
    /// All later queue, prepared SQL, prompt-transfer and ACK uses share it.
    pub(crate) fn with_control(
        mut self,
        control: Arc<dyn intent_core::repository_request::RepositoryMonitorEventSource>,
    ) -> Self {
        self.controls.push(control);
        self
    }

    fn with_controls(
        controls: &[&Arc<dyn intent_core::repository_request::RepositoryMonitorEventSource>],
        action: &mut CacheAdmissionAction<'_>,
    ) -> CredentialResult<()> {
        let Some((first, rest)) = controls.split_first() else {
            return action();
        };
        with_monitor_control(first.as_ref(), &mut || Self::with_controls(rest, action))
    }

    fn with_groups(
        deliveries: &[&Self],
        checks: &[&CacheRevalidationCheck<'_>],
        action: &mut CacheRevalidationAction<'_>,
    ) -> CredentialResult<()> {
        let Some(first) = deliveries.first() else {
            return action(&|| {
                for check in checks {
                    check()?;
                }
                Ok(())
            });
        };
        let end = deliveries.partition_point(|delivery| delivery.cache == first.cache);
        (first.check)(&deliveries[..end], &mut |check| {
            let mut all = checks.to_vec();
            all.push(check);
            Self::with_groups(&deliveries[end..], &all, action)
        })
    }
}
enum DeliveryObservation {
    Complete(Arc<ObservationReceipt>),
    Partial(ObservationTicket),
}
enum DeliveryReceipt<K> {
    Resource {
        key: CacheKey<K>,
        connection: ConnectionObservations,
        observation: DeliveryObservation,
    },
    Page {
        connection: ConnectionObservations,
        list: ListObservations,
        targets: Vec<ReviewTarget>,
    },
}
fn delivery<K: Eq + Hash + Send + Sync + 'static, L: Send + 'static, T: Send + 'static>(
    cache: Weak<Mutex<CacheMap<K, L, T>>>,
    key: CacheKey<K>,
    connection: ConnectionObservations,
    observation: DeliveryObservation,
) -> CacheDelivery {
    delivery_receipt(
        cache,
        DeliveryReceipt::Resource {
            key,
            connection,
            observation,
        },
    )
}

fn page_delivery<K: Eq + Hash + Send + Sync + 'static, L: Send + 'static, T: Send + 'static>(
    cache: &SharedCache<K, L, T>,
    connection: &ConnectionObservations,
    list: &ListObservations,
    targets: Vec<ReviewTarget>,
) -> CacheDelivery {
    delivery_receipt(
        Arc::downgrade(cache),
        DeliveryReceipt::Page {
            connection: connection.clone(),
            list: list.clone(),
            targets,
        },
    )
}

fn delivery_receipt<K: Eq + Hash + Send + Sync + 'static, L: Send + 'static, T: Send + 'static>(
    cache: Weak<Mutex<CacheMap<K, L, T>>>,
    receipt: DeliveryReceipt<K>,
) -> CacheDelivery {
    let identity = cache.as_ptr().addr();
    CacheDelivery {
        cache: identity,
        controls: Vec::new(),
        receipt: Arc::new(receipt),
        check: Arc::new(move |deliveries, action| {
            let cache = cache.upgrade().ok_or(RepositoryCredentialError::Retired)?;
            let locked = cache
                .lock()
                .map_err(|_| RepositoryCredentialError::Indeterminate)?;
            let mut checks: Vec<Box<CacheRevalidationCheck<'static>>> = Vec::new();
            for delivery in deliveries {
                if delivery.cache != identity {
                    return Err(RepositoryCredentialError::BoundaryMismatch);
                }
                let receipt = delivery
                    .receipt
                    .downcast_ref::<DeliveryReceipt<K>>()
                    .ok_or(RepositoryCredentialError::BoundaryMismatch)?;
                match receipt {
                    DeliveryReceipt::Resource {
                        key,
                        connection,
                        observation,
                    } => {
                        let Some(CacheSlot::Qualified(slot)) = locked.get(key) else {
                            return Err(RepositoryCredentialError::Retired);
                        };
                        if !slot.observations.belongs_to(connection) {
                            return Err(RepositoryCredentialError::Retired);
                        }
                        let check = match observation {
                            DeliveryObservation::Complete(observation) => {
                                if !slot.observations.can_serve(observation, connection) {
                                    return Err(RepositoryCredentialError::Retired);
                                }
                                slot.observations.revalidate_receipt(observation)
                            }
                            DeliveryObservation::Partial(ticket) => {
                                slot.observations
                                    .validate(ticket)
                                    .map_err(|_| RepositoryCredentialError::Retired)?;
                                slot.observations.revalidate_ticket(ticket)
                            }
                        };
                        checks.push(Box::new(move || {
                            check
                                .validate()
                                .map_err(|_| RepositoryCredentialError::Retired)
                        }));
                    }
                    DeliveryReceipt::Page {
                        connection,
                        list,
                        targets,
                    } => {
                        if !targets.iter().all(|target| {
                            matches!(locked.get(&CacheKey::Qualified(Box::new(connection.key(target.clone())))),
                                Some(CacheSlot::Qualified(slot)) if slot.observations.belongs_to(connection))
                        }) {
                            return Err(RepositoryCredentialError::Retired);
                        }
                        let list = list.clone();
                        let targets = targets.clone();
                        checks.push(Box::new(move || {
                            list.validate_start()
                                .map_err(|_| RepositoryCredentialError::Retired)?;
                            for target in &targets {
                                list.validate_row(target)
                                    .map_err(|_| RepositoryCredentialError::Retired)?;
                            }
                            Ok(())
                        }));
                    }
                }
            }
            let check = || {
                for check in &checks {
                    check()?;
                }
                Ok(())
            };
            check()?;
            action(&check)
        }),
    }
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
    if let (Some(managed), Err(_)) = (access.managed(), &outcome) {
        return Err(managed_failure(
            cache,
            managed,
            ticket,
            outcome.err().unwrap(),
            quota,
            attribution.as_deref(),
            (true, access.observer()),
        ));
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

/// Only the original provider's guarded disposition changes eligibility.
/// Section errors do not change completeness of a retained detail payload.
fn managed_failure<K, L, T>(
    cache: &SharedCache<K, L, T>,
    managed: &ManagedCacheRequest<'_>,
    ticket: ObservationTicket,
    error: intent_sourcecontrol::Error,
    quota: RateLimitStatus,
    attribution: Option<&RepositoryResponseAttribution>,
    (disables_detail_reuse, observer): (bool, Option<&CacheDenialObserver<'_>>),
) -> CacheFailure
where
    K: Eq + Hash + Send,
    L: Send,
    T: Send,
{
    let request = &managed.request;
    let mut locked = cache.lock().unwrap();
    if let Some(CacheSlot::Qualified(slot)) = locked.get_mut(&request.key()) {
        if slot.observations.belongs_to(request.connection) {
            if let Some(attribution) = attribution {
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
                                D::CurrentProjectDenial => Some(ProviderFailureKind::ProjectDenied),
                                D::CurrentResourceDenial => {
                                    Some(ProviderFailureKind::ResourceDenied)
                                }
                                D::NoDenial => None,
                                D::NotApplied | D::Unattributed | D::Indeterminate => return Ok(()),
                            };
                            let ticket = ticket
                                .take()
                                .ok_or(RepositoryCredentialError::Indeterminate)?;
                            if slot.observations.failure_kind(ticket, kind).is_ok() {
                                observe_denial(
                                    d,
                                    request.connection,
                                    &request.target.repository,
                                    CacheDenialCoverage::Resource(request.target.clone()),
                                    observer,
                                );
                                if disables_detail_reuse {
                                    slot.reuse_allowed = false;
                                }
                            }
                            Ok(())
                        })
                });
            }
        }
    }
    // Application refusal must not erase the response's actual error or quota.
    CacheFailure {
        cause: CacheError::Provider(error),
        quota,
    }
}

/// Uncached fixed-resource sections share the original detail slot's denial
/// and eviction fences. Success never installs a payload, extends its age, or
/// recovers detail/summary coverage. Each provider call owns its own receipt.
pub(crate) async fn read_managed_review_section<T, F, Fut>(
    cache: &PrCache,
    request: &ManagedCacheRequest<'_>,
    monitored: &HashSet<PrKey>,
    provider: F,
) -> Result<CacheRead<T>, CacheFailure>
where
    T: Send,
    F: FnOnce() -> Fut,
    Fut: Future<Output = RepositoryProviderRead<T>>,
{
    read_review_section_access(cache, DetailAccess::Managed(request), monitored, || async {
        provider().await.into()
    })
    .await
}

/// Original managed read plus a guarded, notification-only denial observer.
pub(crate) async fn read_managed_review_section_observed<T, F, Fut>(
    cache: &PrCache,
    request: &ManagedCacheRequest<'_>,
    observer: &CacheDenialObserver<'_>,
    monitored: &HashSet<PrKey>,
    provider: F,
) -> Result<CacheRead<T>, CacheFailure>
where
    T: Send,
    F: FnOnce() -> Fut,
    Fut: Future<Output = RepositoryProviderRead<T>>,
{
    read_review_section_access(
        cache,
        DetailAccess::Observed(request, observer),
        monitored,
        || async { provider().await.into() },
    )
    .await
}

async fn read_review_section_access<T, F, Fut>(
    cache: &PrCache,
    access: DetailAccess<'_, '_>,
    monitored: &HashSet<PrKey>,
    provider: F,
) -> Result<CacheRead<T>, CacheFailure>
where
    T: Send,
    F: FnOnce() -> Fut,
    Fut: Future<Output = DetailOutcome<T>>,
{
    let request = access.request();
    if request.target.kind == RepositoryResourceKind::Issue {
        return Err(CacheFailure::ineligible(
            Ineligible::DifferentSlot,
            RateLimitStatus::default(),
        ));
    }
    let Started::Read { ticket, .. } = start_access(cache, access, Duration::ZERO, |c| {
        retain_pr_cache(c, monitored, Instant::now());
    })?
    else {
        unreachable!("zero age never returns a cache hit")
    };
    let DetailOutcome {
        value,
        quota,
        attribution,
    } = provider().await;
    let value = match value {
        Ok(value) => value,
        Err(error) => {
            if let Some(managed) = access.managed() {
                return Err(managed_failure(
                    cache,
                    managed,
                    ticket,
                    error,
                    quota,
                    attribution.as_deref(),
                    (false, access.observer()),
                ));
            }
            if request.check(quota).is_ok() {
                let mut cache = cache.lock().unwrap();
                if let Some(CacheSlot::Qualified(slot)) = cache.get_mut(&request.key()) {
                    let kind = match &error {
                        intent_sourcecontrol::Error::Provider(failure) => Some(failure.kind),
                        _ => None,
                    };
                    let _ = slot.observations.failure_kind(ticket, kind);
                }
            }
            return Err(CacheFailure {
                cause: CacheError::Provider(error),
                quota,
            });
        }
    };
    access.check(quota)?;
    let mut locked = cache.lock().unwrap();
    let slots = &mut *locked;
    access.response_current(quota, attribution.as_deref(), || {
        let Some(CacheSlot::Qualified(slot)) = slots.get(&request.key()) else {
            return Err(CacheFailure::ineligible(Ineligible::DifferentSlot, quota));
        };
        slot.observations
            .validate(&ticket)
            .map_err(|e| CacheFailure::ineligible(e, quota))?;
        access.check_in_action(quota)?;
        retain_pr_cache(slots, monitored, Instant::now());
        Ok(CacheRead {
            value,
            quota,
            fetched: true,
            delivery: delivery(
                Arc::downgrade(cache),
                request.key(),
                request.connection.clone(),
                DeliveryObservation::Partial(ticket),
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

/// Original managed read plus a guarded, notification-only denial observer.
pub(crate) async fn read_managed_review_observed<P, PF, F, FF>(
    cache: &PrCache,
    request: &ManagedCacheRequest<'_>,
    observer: &CacheDenialObserver<'_>,
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
        DetailAccess::Observed(request, observer),
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
            let fingerprint = PrFingerprint::of(&details.review, None);
            if fingerprint.detects_changes()
                && fingerprint == PrFingerprint::of(&old.details.review, None)
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

/// The native control contributes a lifetime, never forge authority. Keep the
/// original credential error without nesting another account/cache lock.
pub(crate) fn with_monitor_control(
    control: &dyn intent_core::repository_request::RepositoryMonitorEventSource,
    action: &mut CacheAdmissionAction<'_>,
) -> CredentialResult<()> {
    let mut result = None;
    control
        .with_current(&mut || {
            result = Some(action());
            Ok(())
        })
        .map_err(|_| RepositoryCredentialError::Retired)?;
    result.ok_or(RepositoryCredentialError::Retired)?
}
