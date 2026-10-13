//! Explicit GitLab reads on one original native socket. A scope is independent
//! of Git remotes/selection and never grants a repository write.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use intent_core::repository_request::{
    RepositoryContextReleased as Released, RepositoryReadReplyKind, RepositoryReadRequestScope,
    RepositoryResourceBoundQuery as Bound, RepositoryResourceCapture as Capture,
    RepositoryResourceDetailQuery as Detail, RepositoryResourceFailure as Failure,
    RepositoryResourceFrame as Frame, RepositoryResourceInstance as Instance,
    RepositoryResourceOutcome as Outcome, RepositoryResourceQuery as Query,
    RepositoryResourceQuota as Quota, RepositoryResourceResult as ReadResult,
    RepositoryResourceRetired as Retired, RepositoryResourceRetirements,
};
use intent_core::repository_resource_pages::RepositoryReadContext;
use intent_core::repository_resource_pages::RepositoryResourceEvent;
use intent_core::{
    BoxFuture, Error, ExecutionScope, RepositoryAvailability, RepositoryContextRevision,
    RepositoryProvider, RepositoryResourceKind, Result, ReviewTarget,
};
use intent_sourcecontrol::{error::ProviderFailureKind, GitlabInstance};
use intent_store::{RepositoryAuthoritySnapshot, RepositoryLifecycleKey};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use super::{Connection, Services};
use crate::observation_adapter::ConnectionObservations;
use crate::pr_monitor::qualified_cache::{
    CacheAdmissionGuard, CacheDelivery, CacheError, CacheFailure, CacheRequest, ManagedCacheRequest,
};
use crate::repository_admission::lifecycle::{RepositorySourceLifetime, RepositorySubscription};
use crate::repository_admission::AdmissionError;
use crate::repository_credentials::authority::{
    CredentialFuture, RepositoryAuthority, RepositoryAuthorityFence, RepositoryAuthorityRequest,
    RepositoryCredentialTransport,
};
use crate::repository_credentials::read::{
    RepositoryCollectionTarget, RepositoryReadOperation, RepositoryResponseAttribution,
    RepositoryResponseDisposition,
};
use crate::repository_credentials::{
    self as credentials, RepositoryCredentialError, RepositoryCredentialUse,
};
use crate::settings_registry::SettingsSnapshot;
use crate::source_control_auth_ops::repository_owner::{
    RepositoryConnectionFacts, RepositoryReadEligibility, RepositorySettledConnection,
};
use crate::SettingsRegistry;

#[path = "native_resource_read/cursors.rs"]
mod cursors;
#[path = "native_resource_read/known.rs"]
pub(crate) mod known;
#[path = "native_resource_read/observations.rs"]
mod observations;
#[path = "native_resource_read/output.rs"]
mod output;
#[path = "native_resource_read/pages.rs"]
pub(crate) mod pages;
#[path = "native_resource_read/scoped.rs"]
pub(crate) mod scoped;

const LIMIT: usize = 64;
const TTL: Duration = Duration::from_secs(300);
const FRAME_TTL: Duration = Duration::from_secs(15);
const ACQUIRE: Duration = Duration::from_secs(5);
const READ: Duration = Duration::from_secs(10);

fn unavailable() -> Error {
    Error::Forbidden("Repository resource read unavailable".into())
}
fn denied(_: impl std::fmt::Debug) -> Error {
    unavailable()
}
fn credential(_: impl std::fmt::Debug) -> RepositoryCredentialError {
    RepositoryCredentialError::AuthorityDenied
}

pub(crate) struct Capacity {
    leases: Arc<Semaphore>,
    frames: Arc<Semaphore>,
}
impl Default for Capacity {
    fn default() -> Self {
        Self {
            leases: Arc::new(Semaphore::new(LIMIT)),
            frames: Arc::new(Semaphore::new(LIMIT)),
        }
    }
}
#[derive(Default)]
struct Feed {
    taken: bool,
    closed: bool,
    sequence: u64,
    terminal: Option<Retired>,
    notices: VecDeque<RepositoryResourceEvent>,
    leases: HashMap<String, Arc<Lease>>,
}
pub(super) struct ConnectionState {
    feed: Mutex<Feed>,
    groups: Mutex<Vec<Weak<observations::Group>>>,
    notify: Notify,
    leases: Arc<Semaphore>,
    frames: Arc<Semaphore>,
}
impl Default for ConnectionState {
    fn default() -> Self {
        Self {
            feed: Mutex::default(),
            groups: Mutex::default(),
            notify: Notify::new(),
            leases: Arc::new(Semaphore::new(LIMIT)),
            frames: Arc::new(Semaphore::new(LIMIT)),
        }
    }
}
impl ConnectionState {
    fn check(&self) -> Result<()> {
        let f = self.feed.lock().map_err(denied)?;
        if f.closed || !f.taken {
            Err(unavailable())
        } else {
            Ok(())
        }
    }
    pub(super) fn close(&self) {
        let leases = {
            let mut f = self
                .feed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if f.closed {
                return;
            }
            f.closed = true;
            f.sequence = f.sequence.checked_add(1).unwrap_or(f.sequence);
            f.notices.clear();
            f.terminal = Some(Retired {
                read_lifetime_ids: Vec::new(),
                sequence: f.sequence.to_string(),
                all_retired: true,
                terminal: true,
            });
            f.leases.drain().map(|(_, l)| l).collect::<Vec<_>>()
        };
        for lease in leases {
            lease.end();
        }
        self.notify.notify_waiters();
    }
    fn retire(&self, id: &str) {
        let (lease, overflow) = {
            let mut f = self
                .feed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let lease = f.leases.remove(id);
            let overflow = if lease.is_some() && !f.closed {
                if let Some(seq) = f
                    .sequence
                    .checked_add(1)
                    .filter(|_| f.notices.len() < LIMIT)
                {
                    f.sequence = seq;
                    f.notices
                        .push_back(RepositoryResourceEvent::Retired(Retired {
                            read_lifetime_ids: vec![id.into()],
                            sequence: seq.to_string(),
                            all_retired: false,
                            terminal: false,
                        }));
                    false
                } else {
                    true
                }
            } else {
                false
            };
            (lease, overflow)
        };
        if let Some(lease) = lease {
            lease.end();
        }
        if overflow {
            self.close();
        }
        self.notify.notify_waiters();
    }
    fn retire_all(&self) {
        let ids = self
            .feed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .leases
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for id in ids {
            self.retire(&id);
        }
    }
}
struct Receiver {
    connection: Weak<Connection>,
    settings: Option<tokio::sync::watch::Receiver<crate::settings_registry::SettingsChanged>>,
    events: Option<crate::Subscription>,
}
impl Drop for Receiver {
    fn drop(&mut self) {
        if let Some(c) = self.connection.upgrade() {
            c.resource.close();
        }
    }
}
impl RepositoryResourceRetirements for Receiver {
    fn next(&mut self) -> BoxFuture<'_, Option<Retired>> {
        Box::pin(async move {
            loop {
                match self.next_event().await? {
                    RepositoryResourceEvent::Retired(notice) => return Some(notice),
                    RepositoryResourceEvent::Invalidated(_) => {}
                }
            }
        })
    }
    fn next_event(&mut self) -> BoxFuture<'_, Option<RepositoryResourceEvent>> {
        Box::pin(async move {
            let c = self.connection.upgrade()?;
            loop {
                let wake = c.resource.notify.notified();
                tokio::pin!(wake);
                wake.as_mut().enable();
                {
                    let mut f = c
                        .resource
                        .feed
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if let Some(t) = f.terminal.take() {
                        return Some(RepositoryResourceEvent::Retired(t));
                    }
                    if f.closed {
                        return None;
                    }
                    if let Some(n) = f.notices.pop_front() {
                        return Some(n);
                    }
                }
                tokio::select! {
                    () = wake => {},
                    changed = async { match &mut self.settings { Some(s) => s.changed().await, None => std::future::pending().await } } => {
                        if changed.is_err() { c.resource.close(); } else { c.resource.retire_all(); }
                    },
                    event = async { match &mut self.events { Some(s) => s.recv_delivery().await, None => std::future::pending().await } } => {
                        match event { Some(crate::Delivery::Batch(_)) => c.resource.retire_all(), _ => c.resource.close() }
                    }
                }
            }
        })
    }
}
pub(super) fn take_retirements(c: &Connection) -> Option<Box<dyn RepositoryResourceRetirements>> {
    let mut f = c.resource.feed.lock().ok()?;
    if f.taken || f.closed {
        return None;
    }
    f.taken = true;
    Some(Box::new(Receiver {
        connection: c.weak.clone(),
        settings: c.services.settings_registry.as_ref().map(|s| s.subscribe()),
        events: c.services.event_bus.as_ref().map(|bus| {
            bus.subscribe(crate::SubscriptionFilter {
                event_types: vec!["sourceControl:auth-changed".into()],
                ..Default::default()
            })
        }),
    }))
}

struct Lease {
    id: String,
    context: RepositoryReadContext,
    scope: ExecutionScope,
    deadline: Instant,
    authority: scoped::Authority,
    lifetime: RepositorySourceLifetime,
    _registration: RepositorySubscription,
    _subscription: RepositorySubscription,
    _local: OwnedSemaphorePermit,
    _global: OwnedSemaphorePermit,
    settings: (Arc<SettingsRegistry>, Arc<SettingsSnapshot>),
    facts: RepositoryConnectionFacts,
    settled: RepositorySettledConnection,
    observations: ConnectionObservations,
    _group: Arc<observations::Group>,
    reads: AtomicUsize,
    cursors: Mutex<cursors::Cursors>,
    published: AtomicBool,
}
impl Lease {
    fn check(&self) -> Result<()> {
        if Instant::now() >= self.deadline {
            return Err(unavailable());
        }
        self.lifetime.retirement().check_current().map_err(denied)
    }
    fn end(&self) {
        self.lifetime.retirement().end_scope();
    }
    fn revision(&self) -> RepositoryContextRevision {
        RepositoryContextRevision::new(&self.id, 1)
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        self.end();
    }
}

struct Request {
    base: Arc<super::Request>,
    frame: Frame,
    weak: Weak<Self>,
    used: AtomicBool,
    consumed: AtomicBool,
    output_admitted: Arc<AtomicBool>,
    state: Mutex<State>,
    permits: Option<(OwnedSemaphorePermit, OwnedSemaphorePermit)>,
    deadline: Instant,
}
#[derive(Clone)]
enum State {
    Empty,
    Public,
    Capture(Arc<Lease>),
    Reading(Arc<Lease>),
    Read(
        Arc<Lease>,
        Arc<RepositoryReadEligibility>,
        Option<Arc<RepositoryResponseAttribution>>,
        bool,
        Option<CacheDelivery>,
        Option<Arc<RepositoryCollectionTarget>>,
    ),
}
tokio::task_local! { static RESOURCE_REQUEST: Arc<Request>; }

pub(super) fn capture_frame(c: &Connection, frame: Frame) -> Arc<dyn RepositoryReadRequestScope> {
    let captured = (|| {
        c.entered().map_err(denied)?;
        c.resource.check()?;
        let local = c
            .resource
            .frames
            .clone()
            .try_acquire_owned()
            .map_err(denied)?;
        let global = c
            .services
            .repository_resource_capacity
            .frames
            .clone()
            .try_acquire_owned()
            .map_err(denied)?;
        let (lifetime, registration) = c.new_lifetime().map_err(denied)?;
        let subscription = lifetime
            .subscribe(
                &c.services.store,
                c.caller.caller(),
                &keys(&frame.context()),
            )
            .map_err(denied)?;
        Ok::<_, Error>((lifetime, registration, subscription, local, global))
    })();
    let (lifetime, registration, subscriptions, permits) = match captured {
        Ok((lifetime, registration, subscription, local, global)) => (
            Some(lifetime),
            Some(registration),
            vec![subscription],
            Some((local, global)),
        ),
        Err(_) => (None, None, Vec::new(), None),
    };
    // This private carrier reuses the original Wire/Store authority routine.
    // It never installs an inventory query or consumes an inventory lease.
    let base = Arc::new_cyclic(|weak| super::Request {
        connection: c.weak.upgrade().expect("owned original connection"),
        weak: weak.clone(),
        lifetime,
        _registration: registration,
        state: Mutex::new(super::RequestState::Ordinary),
        completed: AtomicBool::new(false),
        consumed: AtomicBool::new(false),
        read_lane: Mutex::new(None),
        subscriptions: Mutex::new(subscriptions),
        queued_query: None,
        monitor_receipt: c.monitor.capture(),
    });
    let request = Arc::new_cyclic(|weak| Request {
        base,
        frame,
        weak: weak.clone(),
        used: AtomicBool::new(false),
        consumed: AtomicBool::new(false),
        state: Mutex::new(State::Empty),
        output_admitted: Arc::new(AtomicBool::new(false)),
        permits,
        deadline: Instant::now() + FRAME_TTL,
    });
    if let Some(lifetime) = &request.base.lifetime {
        let weak = Arc::downgrade(&request);
        let stop = lifetime.retirement();
        let deadline = request.deadline;
        // caller-binding: allow — only retires the original request lifetime; no service capability calls
        tokio::spawn(async move {
            tokio::select! { () = stop.native_cancelled() => return, () = tokio::time::sleep_until(deadline) => {} }
            if let Some(r) = weak.upgrade() {
                r.finish();
            }
        });
    }
    request
}
fn keys(context: &RepositoryReadContext) -> Vec<RepositoryLifecycleKey> {
    let mut keys = vec![
        RepositoryLifecycleKey::Database,
        RepositoryLifecycleKey::WireAuthority,
    ];
    if let Some(workspace) = context.workspace_id() {
        keys.push(RepositoryLifecycleKey::Workspace(workspace.clone()));
    }
    keys
}
impl Request {
    fn current(services: &Services, frame: &Frame) -> Result<Arc<Self>> {
        let r = RESOURCE_REQUEST.try_with(Clone::clone).map_err(denied)?;
        if !std::ptr::eq(r.base.connection.services.as_ref(), services)
            || &r.frame != frame
            || r.used.swap(true, Ordering::AcqRel)
        {
            return Err(unavailable());
        }
        r.check()?;
        Ok(r)
    }
    fn check(&self) -> Result<()> {
        self.base.check().map_err(denied)?;
        self.base.connection.resource.check()?;
        if self.permits.is_none() || Instant::now() >= self.deadline {
            return Err(unavailable());
        }
        Ok(())
    }
    fn finish(&self) {
        self.base.finish();
        let state = std::mem::replace(
            &mut *self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            State::Empty,
        );
        let retire = match state {
            State::Capture(l) if !l.published.load(Ordering::Acquire) => Some(l),
            State::Reading(l) | State::Read(l, _, _, _, _, _)
                if !self.consumed.load(Ordering::Acquire) =>
            {
                Some(l)
            }
            _ => None,
        };
        if let Some(l) = retire {
            self.base.connection.resource.retire(&l.id);
        }
    }
    async fn authority(&self) -> Result<scoped::Authority> {
        scoped::authority(self).await
    }
    async fn validate(&self, lease: &Lease) -> Result<()> {
        self.check()?;
        lease.check()?;
        if self.authority().await? != lease.authority {
            return Err(unavailable());
        }
        self.with_scope(lease, || Ok(()))
    }
    fn with_scope<T>(
        &self,
        lease: &Lease,
        action: impl FnOnce() -> credentials::Result<T>,
    ) -> Result<T> {
        self.check()?;
        lease.check()?;
        self.base
            .connection
            .parent
            .native_dispatch(|| {
                self.base
                    .lifetime
                    .as_ref()
                    .ok_or(AdmissionError::Retired)?
                    .retirement()
                    .native_dispatch(|| {
                        lease.lifetime.retirement().native_dispatch(|| {
                            if Instant::now() >= lease.deadline || Instant::now() >= self.deadline {
                                return Err(AdmissionError::Retired);
                            }
                            lease
                                .settings
                                .0
                                .with_original_snapshot(&lease.settings.1, |current| {
                                    if current {
                                        Ok(action())
                                    } else {
                                        Err(AdmissionError::Retired)
                                    }
                                })
                        })
                    })
            })
            .map_err(denied)?
            .map_err(denied)
    }
    fn lease(&self, id: &str) -> Result<Arc<Lease>> {
        let c = &self.base.connection;
        self.check()?;
        let lease = c
            .resource
            .feed
            .lock()
            .map_err(denied)?
            .leases
            .get(id)
            .cloned()
            .ok_or_else(unavailable)?;
        if lease.context != self.frame.context() || !lease.published.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        lease.check()?;
        Ok(lease)
    }
}
impl Drop for Request {
    fn drop(&mut self) {
        self.finish();
    }
}
impl RepositoryReadRequestScope for Request {
    fn bind_private_output(
        &self,
        frame: &str,
    ) -> Result<Option<Arc<dyn intent_core::repository_request::RepositoryPrivateOutput>>> {
        output::bind(self, frame)
    }

    fn scope<'a>(&'a self, body: BoxFuture<'a, ()>) -> BoxFuture<'a, ()> {
        Box::pin(RESOURCE_REQUEST.scope(self.weak.upgrade().expect("owned original frame"), body))
    }
    fn retire(&self) {
        self.finish();
    }
    fn deliver<'a>(
        &'a self,
        kind: RepositoryReadReplyKind,
        transfer: &'a mut (dyn FnMut() -> Result<()> + Send),
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            enum Delivery {
                Public,
                Capture(Arc<Lease>),
                Read(
                    Arc<Lease>,
                    Arc<RepositoryReadEligibility>,
                    Option<Arc<RepositoryResponseAttribution>>,
                    bool,
                    Option<CacheDelivery>,
                    Option<Arc<RepositoryCollectionTarget>>,
                ),
            }
            let state = {
                let s = self.state.lock().map_err(denied)?;
                match &*s {
                    State::Public => Delivery::Public,
                    State::Capture(l) => Delivery::Capture(l.clone()),
                    State::Read(lease, eligibility, attribution, failed, delivery, collection) => {
                        Delivery::Read(
                            lease.clone(),
                            eligibility.clone(),
                            attribution.clone(),
                            *failed,
                            delivery.clone(),
                            collection.clone(),
                        )
                    }
                    _ => return Err(unavailable()),
                }
            };
            if matches!(state, Delivery::Public) {
                // Only a sanitized service error or explicit release is public.
                if kind == RepositoryReadReplyKind::Result
                    && !matches!(self.frame, Frame::Release(_) | Frame::ReleaseScoped(_))
                {
                    return Err(unavailable());
                }
                if self.consumed.swap(true, Ordering::AcqRel) {
                    return Err(unavailable());
                }
                return transfer();
            }
            if kind != RepositoryReadReplyKind::Result {
                return Err(unavailable());
            }
            let lease = match &state {
                Delivery::Capture(l) | Delivery::Read(l, _, _, _, _, _) => l.clone(),
                Delivery::Public => unreachable!(),
            };
            let result = tokio::time::timeout(ACQUIRE, async {
                let _legacy = self.base.connection.caller.legacy_lease().await.map_err(denied)?;
                self.validate(&lease).await?;
                // Snapshot all private records before taking consuming fences.
                let response = if let Delivery::Read(_,_,a,_,_,_) = &state { a.clone() } else { None };
                let mut transferred = None;
                let mut guarded = |check: &crate::pr_monitor::qualified_cache::CacheRevalidationCheck<'_>| self.with_scope(&lease, || {
                    let mut action = || {
                        check()?;
                        if self.consumed.swap(true,Ordering::AcqRel) { return Err(RepositoryCredentialError::Retired); }
                        let actual = transfer();
                        if actual.is_ok() {
                            lease.published.store(true,Ordering::Release);
                            self.output_admitted.store(true,Ordering::Release);
                        }
                        transferred = Some(actual);
                        Ok(())
                    };
                    match &state {
                        Delivery::Capture(_) => RepositoryConnectionFacts::with_native_context_current(Some(&lease.facts), |current| {
                            if current { action() } else { Err(RepositoryCredentialError::Retired) }
                        }),
                        Delivery::Read(_,eligibility,_,failure,_,collection) => {
                            if *failure {
                                if let Some(response) = &response {
                                    let mut accepted_rejection = false;
                                    with_response(eligibility, response, self.frame.resource_target(), collection.as_deref(), &mut |disposition| {
                                        accepted_rejection = disposition == RepositoryResponseDisposition::AcceptedCredentialRejection;
                                        if matches!(disposition, RepositoryResponseDisposition::NotApplied | RepositoryResponseDisposition::Unattributed | RepositoryResponseDisposition::Indeterminate) { return Err(RepositoryCredentialError::Retired); }
                                        if accepted_rejection { action()?; }
                                        Ok(())
                                    })?;
                                    // An actual accepted 401 is an error event about the original
                                    // request, not a grant or successful private payload.
                                    if accepted_rejection { return Ok(()); }
                                }
                            }
                            eligibility.with_current(&mut action)
                        }
                        Delivery::Public => unreachable!(),
                    }
                }).map_err(credential);
                if let Delivery::Read(_,_,_,false,Some(delivery),_) = &state {
                    delivery.with_revalidation(&mut guarded).map_err(denied)?;
                } else { guarded(&|| Ok(())).map_err(denied)?; }
                transferred.ok_or_else(unavailable)?
            }).await.map_err(denied).and_then(|r|r);
            if result.is_err() {
                self.base.connection.resource.retire(&lease.id);
            }
            result
        })
    }
}

fn entry<'a, T: Send + 'a>(
    services: &'a Services,
    frame: &Frame,
    body: impl FnOnce(Arc<Request>) -> BoxFuture<'a, Result<T>> + Send + 'a,
) -> BoxFuture<'a, Result<T>> {
    let captured = Request::current(services, frame);
    Box::pin(async move {
        let r = captured?;
        let result = body(r.clone()).await;
        if result.is_err() {
            let previous = std::mem::replace(&mut *r.state.lock().map_err(denied)?, State::Public);
            if let State::Capture(l) | State::Reading(l) | State::Read(l, _, _, _, _, _) = previous
            {
                r.base.connection.resource.retire(&l.id);
            }
        }
        result
    })
}
pub(crate) fn capture(services: &Services, q: Query) -> BoxFuture<'_, Result<Capture>> {
    entry(services, &Frame::Capture(q), |r| {
        Box::pin(async move {
            tokio::time::timeout(ACQUIRE, capture_inner(&r))
                .await
                .map_err(denied)?
        })
    })
}
async fn capture_inner(r: &Arc<Request>) -> Result<Capture> {
    let c = &r.base.connection;
    let _legacy = c.caller.legacy_lease().await.map_err(denied)?;
    let local = c
        .resource
        .leases
        .clone()
        .try_acquire_owned()
        .map_err(denied)?;
    let global = c
        .services
        .repository_resource_capacity
        .leases
        .clone()
        .try_acquire_owned()
        .map_err(denied)?;
    let (lifetime, registration) = c.new_lifetime().map_err(denied)?;
    let subscription = lifetime
        .subscribe(
            &c.services.store,
            c.caller.caller(),
            &keys(&r.frame.context()),
        )
        .map_err(denied)?;
    let authority = r.authority().await?;
    let registry = c
        .services
        .settings_registry
        .clone()
        .ok_or_else(unavailable)?;
    let settings = registry.snapshot();
    // These original-owner reads are metadata only. No auth probe or secret read.
    let facts = c
        .services
        .gitlab_repository_connection_facts()
        .map_err(denied)?;
    if facts.settled().is_none() {
        return Err(unavailable());
    }
    let settled = c
        .services
        .gitlab_repository_settled_connection()
        .map_err(denied)?;
    if r.authority().await? != authority {
        return Err(unavailable());
    }
    let id = uuid::Uuid::new_v4().to_string();
    let scope = ExecutionScope {
        daemon_id: c.services.daemon_boot_id.clone(),
        authority_scope_id: id.clone(),
        authority_generation: authority.generation()?,
    };
    let group = observations::group(
        c,
        &r.frame.context(),
        &authority,
        &settings,
        &scope,
        &settled,
        matches!(r.frame, Frame::CaptureScoped(_)),
    )?;
    let observations = group.observations.clone();
    let scope = group.scope.clone();
    let instance_base_url = settled.descriptor().instance().as_str().to_owned();
    let lease = Arc::new(Lease {
        id: id.clone(),
        context: r.frame.context(),
        scope: scope.clone(),
        deadline: Instant::now() + TTL,
        authority,
        lifetime,
        _registration: registration,
        _subscription: subscription,
        _local: local,
        _global: global,
        settings: (registry, settings),
        facts,
        settled,
        observations,
        _group: group,
        reads: AtomicUsize::new(0),
        cursors: Mutex::default(),
        published: AtomicBool::new(false),
    });
    r.with_scope(&lease, || {
        RepositoryConnectionFacts::with_native_context_current(Some(&lease.facts), |current| {
            if current {
                Ok(())
            } else {
                Err(RepositoryCredentialError::Retired)
            }
        })
    })?;
    let sequence = {
        let mut f = c.resource.feed.lock().map_err(denied)?;
        if f.closed {
            return Err(unavailable());
        }
        f.leases.insert(id.clone(), lease.clone());
        f.sequence
    };
    *r.state.lock().map_err(denied)? = State::Capture(lease.clone());
    let weak = c.weak.clone();
    let watched = id.clone();
    let stop = lease.lifetime.retirement();
    let deadline = lease.deadline;
    // caller-binding: allow — only retires the original resource lease; no service capability calls
    tokio::spawn(async move {
        tokio::select! { ()=stop.native_cancelled()=>{}, ()=tokio::time::sleep_until(deadline)=>{} }
        if let Some(c) = weak.upgrade() {
            c.resource.retire(&watched);
        }
    });
    Ok(Capture {
        read_lifetime_id: id,
        scope,
        revision: lease.revision(),
        expires_after_ms: 300_000,
        retirement_sequence: sequence.to_string(),
        instances: vec![Instance {
            provider: RepositoryProvider::Gitlab,
            instance_base_url,
            availability: RepositoryAvailability::Connected,
        }],
    })
}
pub(crate) fn release(services: &Services, q: Bound) -> BoxFuture<'_, Result<Released>> {
    entry(services, &Frame::Release(q.clone()), move |r| {
        Box::pin(async move { release_inner(&r, &q.read_lifetime_id).await })
    })
}
async fn release_inner(r: &Request, id: &str) -> Result<Released> {
    // Release retains the same fixed original authority acquisition budget.
    tokio::time::timeout(ACQUIRE, r.authority())
        .await
        .map_err(denied)??;
    if let Some(l) = r
        .base
        .connection
        .resource
        .feed
        .lock()
        .map_err(denied)?
        .leases
        .get(id)
    {
        if l.context != r.frame.context() {
            return Err(unavailable());
        }
    }
    r.base.connection.resource.retire(id);
    *r.state.lock().map_err(denied)? = State::Public;
    Ok(Released { released: true })
}

struct ReadAuthority {
    request: Weak<Request>,
    lease: Arc<Lease>,
    expected: RepositoryAuthorityRequest,
}
impl ReadAuthority {
    fn with_current<T>(
        &self,
        action: impl FnOnce() -> credentials::Result<T>,
    ) -> credentials::Result<T> {
        self.request
            .upgrade()
            .ok_or(RepositoryCredentialError::Retired)?
            .with_scope(&self.lease, action)
            .map_err(credential)
    }
    async fn validate(&self) -> credentials::Result<()> {
        let request = self
            .request
            .upgrade()
            .ok_or(RepositoryCredentialError::Retired)?;
        if let Err(e) = request.validate(&self.lease).await {
            request.base.connection.resource.retire(&self.lease.id);
            return Err(credential(e));
        }
        Ok(())
    }
}
struct Fence(Arc<ReadAuthority>);
impl RepositoryAuthorityFence for Fence {
    fn dispatch(
        self: Box<Self>,
        action: &mut (dyn FnMut() -> credentials::Result<()> + Send),
    ) -> credentials::Result<()> {
        self.0.with_current(action)
    }
}
impl RepositoryAuthority for Arc<ReadAuthority> {
    fn revalidate<'a>(
        &'a self,
        expected: &'a RepositoryAuthorityRequest,
    ) -> CredentialFuture<'a, Box<dyn RepositoryAuthorityFence>> {
        Box::pin(async move {
            if expected != &self.expected {
                return Err(RepositoryCredentialError::BoundaryMismatch);
            }
            self.validate().await?;
            Ok(Box::new(Fence(self.clone())) as Box<dyn RepositoryAuthorityFence>)
        })
    }
}
fn valid_repository(repository: &intent_core::RepositoryTarget) -> Result<()> {
    let instance = GitlabInstance::parse(&repository.instance_base_url)
        .map_err(|_| Error::InvalidParams("Invalid resource instance".into()))?;
    let path = &repository.project_path;
    if repository.provider != RepositoryProvider::Gitlab
        || instance.as_str() != repository.instance_base_url
        || !instance.as_str().starts_with("https://")
        || path.len() > 1024
        || path.split('/').count() < 2
        || path.split('/').any(|p| {
            p.is_empty()
                || matches!(p, "." | "..")
                || !p
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
        })
    {
        return Err(Error::InvalidParams(
            "Invalid explicit GitLab target".into(),
        ));
    }
    Ok(())
}
fn valid_target(target: &ReviewTarget) -> Result<()> {
    valid_repository(&target.repository)?;
    if !(1..=9_007_199_254_740_991).contains(&target.number)
        || !matches!(
            target.kind,
            RepositoryResourceKind::MergeRequest | RepositoryResourceKind::Issue
        )
    {
        return Err(Error::InvalidParams(
            "Invalid explicit GitLab target".into(),
        ));
    }
    Ok(())
}

fn with_response(
    eligibility: &RepositoryReadEligibility,
    response: &RepositoryResponseAttribution,
    target: Option<&ReviewTarget>,
    collection: Option<&RepositoryCollectionTarget>,
    action: &mut (dyn FnMut(RepositoryResponseDisposition) -> credentials::Result<()> + Send),
) -> credentials::Result<()> {
    match collection {
        Some(collection) => eligibility.with_collection_response(response, collection, action),
        None => eligibility.with_response(
            response,
            target.ok_or(RepositoryCredentialError::BoundaryMismatch)?,
            action,
        ),
    }
}
fn validate_resource_url(target: &ReviewTarget, url: &str) -> Result<()> {
    let kind = match target.kind {
        RepositoryResourceKind::MergeRequest => "merge_requests",
        RepositoryResourceKind::Issue => "issues",
        RepositoryResourceKind::PullRequest => return Err(unavailable()),
    };
    let expected = format!(
        "{}/{}/-/{}/{}",
        target.repository.instance_base_url, target.repository.project_path, kind, target.number
    );
    if url == expected {
        Ok(())
    } else {
        Err(unavailable())
    }
}
pub(crate) fn detail(services: &Services, q: Detail) -> BoxFuture<'_, Result<ReadResult>> {
    entry(services, &Frame::Detail(q.clone()), move |r| {
        Box::pin(async move { detail_inner(&r, &q.read_lifetime_id, &q.target, q.refresh).await })
    })
}
async fn detail_inner(
    r: &Arc<Request>,
    id: &str,
    target: &ReviewTarget,
    refresh: bool,
) -> Result<ReadResult> {
    valid_target(target)?;
    let lease = r.lease(id)?;
    *r.state.lock().map_err(denied)? = State::Reading(lease.clone());
    if target.repository.instance_base_url != lease.settled.descriptor().instance().as_str() {
        return Err(unavailable());
    }
    lease
        .reads
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
            n.checked_add(1).filter(|n| *n <= LIMIT)
        })
        .map_err(denied)?;
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
        result=tokio::time::timeout(READ,read_inner(r,&lease,target,refresh))=>result.map_err(denied)?,
    }
}

async fn read_inner(
    r: &Arc<Request>,
    lease: &Arc<Lease>,
    target: &ReviewTarget,
    refresh: bool,
) -> Result<ReadResult> {
    let c = &r.base.connection;
    let _legacy = c.caller.legacy_lease().await.map_err(denied)?;
    r.validate(lease).await?;
    let expected = RepositoryAuthorityRequest {
        execution: lease.scope.clone(),
        target: target.repository.clone(),
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
    let bridge: Arc<dyn RepositoryAuthority> = Arc::new(authority.clone());
    let directory = c.services.repository_connection_directory();
    let admit = || {
        directory.admit(
            &lease.settled.selected().binding,
            expected.clone(),
            bridge.clone(),
        )
    };
    let first = admit().map_err(denied)?;
    let eligibility = Arc::new(
        c.services
            .gitlab_repository_read_eligibility(&first)
            .map_err(denied)?,
    );
    let reader = c
        .services
        .gitlab_repository_secret_reader()
        .map_err(denied)?;
    let primary = RepositoryReadOperation::new(
        directory.clone(),
        first,
        reader.clone(),
        READ,
        target.clone(),
    )
    .map_err(denied)?;
    let full = RepositoryReadOperation::new(
        directory.clone(),
        admit().map_err(denied)?,
        reader,
        READ,
        target.clone(),
    )
    .map_err(denied)?;
    let revalidate = || r.check().and_then(|()| lease.check());
    let denials = observations::Denials::new(r, lease);
    let guard: &CacheAdmissionGuard<'_> = &|action| denials.with_current(action);
    let observer: &crate::pr_monitor::qualified_cache::CacheDenialObserver<'_> =
        &|notice| denials.observe(notice);
    let cache = ManagedCacheRequest {
        request: CacheRequest {
            connection: &lease.observations,
            target: target,
            revalidate: &revalidate,
        },
        eligibility: &eligibility,
        with_authority: guard,
    };
    let age = if refresh {
        Duration::ZERO
    } else {
        c.services.pr_cache_max_age()
    };
    let response = Mutex::new(None);
    let invalid_identity = AtomicBool::new(false);
    let identity_matches = |number, url: &str| {
        let matched = number == target.number && validate_resource_url(target, url).is_ok();
        if !matched {
            invalid_identity.store(true, Ordering::Release);
        }
        matched
    };
    let result = match target.kind {
        RepositoryResourceKind::MergeRequest => {
            let read = crate::pr_monitor::qualified_cache::read_managed_review_observed(
                &c.services.pr_cache,
                &cache,
                observer,
                crate::pr_monitor::PrReadPolicy::Serve { max_age: age },
                &HashSet::new(),
                || async {
                    let v = primary.review_details().await.reject_value_unless(|value| {
                        identity_matches(value.review.number, &value.review.url)
                    });
                    *response.lock().unwrap() = Some(v.attribution());
                    let _ = authority.validate().await;
                    v
                },
                || async {
                    let v = full
                        .review_observation()
                        .await
                        .reject_value_unless(|value| {
                            identity_matches(value.details.review.number, &value.details.review.url)
                        });
                    *response.lock().unwrap() = Some(v.attribution());
                    let _ = authority.validate().await;
                    v
                },
            )
            .await;
            read.map(|v| {
                (
                    validate_resource_url(target, &v.value.details.review.url)
                        .and_then(|()| crate::pr_ops::qualified_review_snapshot(target, &v.value))
                        .map(|snapshot| Outcome::MergeRequest { snapshot }),
                    v.quota,
                    v.delivery,
                )
            })
        }
        RepositoryResourceKind::Issue => {
            let read = crate::issue_cache::read_managed_issue_observed(
                &c.services.issue_cache,
                &cache,
                observer,
                age,
                || async {
                    let v = primary
                        .read_issue()
                        .await
                        .reject_value_unless(|value| identity_matches(value.number, &value.url));
                    *response.lock().unwrap() = Some(v.attribution());
                    let _ = authority.validate().await;
                    v
                },
            )
            .await;
            read.map(|v| {
                let value = if v.value.number != target.number
                    || validate_resource_url(target, &v.value.url).is_err()
                {
                    Err(unavailable())
                } else {
                    serde_json::to_value(v.value)
                        .map(|issue| Outcome::Issue { issue })
                        .map_err(denied)
                };
                (value, v.quota, v.delivery)
            })
        }
        RepositoryResourceKind::PullRequest => return Err(unavailable()),
    };
    if invalid_identity.load(Ordering::Acquire) {
        return Err(unavailable());
    }
    let (outcome, rate, failure, delivery) = match result {
        Ok((value, quota, delivery)) => (value?, quota, false, Some(delivery)),
        Err(CacheFailure {
            cause: CacheError::Provider(error),
            quota,
        }) => {
            let (code, status) = failure(&error);
            (Outcome::Failure { code, status }, quota, true, None)
        }
        Err(_) => return Err(unavailable()),
    };
    // Provider failure evidence is retained independently of its own P denial;
    // the final original transfer still classifies and consumes that evidence.
    r.validate(lease).await?;
    *r.state.lock().map_err(denied)? = State::Read(
        lease.clone(),
        eligibility,
        response.into_inner().map_err(denied)?,
        failure,
        delivery,
        None,
    );
    Ok(ReadResult {
        read_lifetime_id: lease.id.clone(),
        scope: lease.scope.clone(),
        revision: lease.revision(),
        target: target.clone(),
        outcome,
        quota: Quota {
            reset_at: rate.reset_at.map(|n| n.to_string()),
            remaining: rate.remaining.map(|n| n.to_string()),
            limit: rate.limit.map(|n| n.to_string()),
        },
    })
}
fn failure(e: &intent_sourcecontrol::Error) -> (Failure, Option<u16>) {
    use intent_sourcecontrol::Error as E;
    match e {
        E::Provider(p) => (
            match p.kind {
                ProviderFailureKind::CredentialRejected => Failure::Authentication,
                ProviderFailureKind::ProjectDenied => Failure::ProjectDenied,
                ProviderFailureKind::ResourceDenied => Failure::ResourceDenied,
                ProviderFailureKind::OptionalRestricted => Failure::OptionalRestricted,
                ProviderFailureKind::OptionalUnavailable => Failure::OptionalUnavailable,
                ProviderFailureKind::Transient => Failure::Transient,
                ProviderFailureKind::Unknown | ProviderFailureKind::WriteUncertain => {
                    Failure::Unknown
                }
            },
            p.status,
        ),
        E::Auth(_) => (Failure::Authentication, None),
        E::RateLimited(_) => (Failure::RateLimited, None),
        E::Api(_) => (Failure::Transient, None),
        E::AdmissionRetired | E::AdmissionUnavailable(_) | E::NotConfigured(_) => {
            (Failure::Unavailable, None)
        }
        E::Unsupported(_) | E::DeviceGrantUnsupported(_) => (Failure::OptionalUnavailable, None),
        _ => (Failure::Unknown, None),
    }
}

#[cfg(test)]
#[path = "native_resource_read/tests.rs"]
mod tests;
