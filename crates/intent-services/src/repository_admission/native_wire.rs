//! Original ordinary-transport repository context. Public handles are correlation,
//! never permission; every acquisition and consuming reply uses the original Wire.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use intent_core::caller::{
    current_caller, current_wire_credential, with_caller, with_wire_credential, WireCredential,
};
use intent_core::repository_request::{
    RepositoryContextBoundQuery, RepositoryContextCapture, RepositoryContextCoverage,
    RepositoryContextQuery, RepositoryContextReleased, RepositoryContextRetired,
    RepositoryReadConnection, RepositoryReadReplyKind, RepositoryReadRequestScope,
    RepositoryReadRetirements, RepositoryWireEntry,
};
use intent_core::{
    BoxFuture, Caller, Error, ExecutionScope, RepositoryContext, RepositoryContextRevision,
    RepositoryRootContext, RepositoryRootId, RepositoryRootKind, Result,
};
use intent_store::{
    RepositoryAuthoritySnapshot, RepositoryLifecycleKey, RepositoryLifecycleObserver,
};
use tokio::sync::{oneshot, Notify, OwnedSemaphorePermit, Semaphore};

use crate::repository_admission::lifecycle::{
    RepositorySourceLifetime, RepositorySubscription, RepositoryWireOrigin,
};
use crate::repository_admission::{
    AdmissionError, AdmissionResult, OriginalRepositoryCaller, RepositoryAuthorityFacts,
    RepositoryAuthorityProvenance, RepositoryEntry, RepositoryRetirement,
};
use crate::repository_admission_git_source::{RepositoryGitSource, RootRecord};
use crate::repository_context_live::{resolver, target_context, SelectionFacts};
use crate::repository_context_reader::{
    read_context_root_with_resolver, GitConfigEnvironment, RepositoryObservedRoot,
};
use crate::settings_registry::SettingsSnapshot;
use crate::source_control_auth_ops::repository_owner::{
    RepositoryAttachmentState, RepositoryConnectionFacts, RepositoryDescriptorState,
};
use crate::{Services, SettingsRegistry};

#[path = "native_review.rs"]
pub(crate) mod review;

#[path = "native_resource_read.rs"]
pub(crate) mod resource;

#[path = "native_selection.rs"]
pub(crate) mod selection;

const LEASE_LIMIT: usize = 64;
const ROOT_LIMIT: usize = 128;
const NOTICE_LIMIT: usize = 64;
const READ_LIMIT: usize = 64;
const WORKER_LIMIT: usize = 2;
const LEASE_TTL: Duration = Duration::from_secs(300);
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);
const READ_TIMEOUT: Duration = Duration::from_secs(10);
const FRAME_TTL: Duration = Duration::from_secs(15);

fn unavailable() -> Error {
    Error::Forbidden("Repository context unavailable".into())
}
fn local(_: impl std::fmt::Debug) -> AdmissionError {
    AdmissionError::Unavailable
}

tokio::task_local! { static NATIVE_REQUEST: Arc<Request>; }

impl Services {
    /// Install the native entry on the exact API allocation before listeners start.
    /// A failed installation leaves qualified reads unavailable; no Agent is needed.
    ///
    /// # Errors
    /// Returns a sanitized error when the original observer cannot be installed.
    pub async fn initialize_repository_wire(self: &Arc<Self>) -> Result<()> {
        self.repository_lifecycle_registry()
            .await
            .map_err(|_| unavailable())?;
        if let Some(old) = self.repository_wire_owner.get() {
            return if old.upgrade().is_some_and(|old| Arc::ptr_eq(&old, self)) {
                Ok(())
            } else {
                Err(unavailable())
            };
        }
        self.repository_wire_owner
            .set(Arc::downgrade(self))
            .map_err(|_| unavailable())
    }
}

pub(crate) fn connection(
    services: &Services,
    entry: RepositoryWireEntry,
) -> Option<Arc<dyn RepositoryReadConnection>> {
    let original = services.repository_wire_owner.get()?.upgrade()?;
    if !std::ptr::eq(services, original.as_ref()) {
        return None;
    }
    let observer: Arc<dyn RepositoryLifecycleObserver> =
        services.repository_lifecycle_registry.clone();
    if !services.store.has_repository_lifecycle_observer(&observer) {
        return None;
    }
    let caller = Arc::new(
        OriginalRepositoryCaller::capture(match entry {
            RepositoryWireEntry::Bearer => RepositoryEntry::Bearer,
            RepositoryWireEntry::AdmittedLocal => RepositoryEntry::AdmittedLocal,
        })
        .ok()?,
    );
    let origin =
        RepositoryWireOrigin::capture(&services.repository_lifecycle_registry, &caller).ok()?;
    Some(Arc::new_cyclic(|weak| Connection {
        services: original,
        caller,
        origin,
        weak: weak.clone(),
        parent: RepositoryRetirement::default(),
        review: review::ConnectionState::default(),
        resource: resource::ConnectionState::default(),
        selection: selection::ConnectionState::default(),
        state: Mutex::new(ConnectionState::default()),
        notify: Notify::new(),
        permits: Arc::new(Semaphore::new(LEASE_LIMIT)),
        workers: Arc::new(Semaphore::new(WORKER_LIMIT)),
        active_jobs: AtomicUsize::new(0),
        jobs_changed: Notify::new(),
        #[cfg(test)]
        git_probe: Mutex::new(None),
    }))
}

struct Connection {
    services: Arc<Services>,
    caller: Arc<OriginalRepositoryCaller>,
    origin: RepositoryWireOrigin,
    weak: Weak<Self>,
    parent: RepositoryRetirement,
    review: review::ConnectionState,
    resource: resource::ConnectionState,
    selection: selection::ConnectionState,
    state: Mutex<ConnectionState>,
    notify: Notify,
    permits: Arc<Semaphore>,
    workers: Arc<Semaphore>,
    active_jobs: AtomicUsize,
    jobs_changed: Notify,
    #[cfg(test)]
    git_probe: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}
#[derive(Default)]
struct ConnectionState {
    feed_taken: bool,
    closed: bool,
    terminal: Option<RepositoryContextRetired>,
    sequence: u64,
    notices: VecDeque<RepositoryContextRetired>,
    leases: HashMap<String, Arc<Lease>>,
}

fn same_credential(a: Option<&WireCredential>, b: Option<&WireCredential>) -> bool {
    match (a, b) {
        (None, None) => true,
        (
            Some(WireCredential::Principal {
                principal_id: a,
                token_hash: ah,
            }),
            Some(WireCredential::Principal {
                principal_id: b,
                token_hash: bh,
            }),
        ) => a == b && ah == bh,
        (
            Some(WireCredential::Legacy {
                principal_id: a,
                authority: aa,
            }),
            Some(WireCredential::Legacy {
                principal_id: b,
                authority: ba,
            }),
        ) => a == b && Arc::ptr_eq(aa, ba),
        _ => false,
    }
}
impl Connection {
    fn entered(&self) -> AdmissionResult<()> {
        if current_caller().as_ref() != Some(self.caller.caller())
            || !same_credential(
                current_wire_credential().as_ref(),
                self.caller.wire_credential(),
            )
        {
            return Err(AdmissionError::Denied);
        }
        self.parent.check_current()?;
        let state = self.state.lock().map_err(local)?;
        if state.closed || !state.feed_taken {
            return Err(AdmissionError::Retired);
        }
        Ok(())
    }
    fn close(&self) {
        let leases = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !state.closed {
                state.closed = true;
                // A terminal all-retired notice remains at MAX if there is
                // no representable successor; the original feed stays closed.
                if let Some(next) = state.sequence.checked_add(1) {
                    state.sequence = next;
                }
                state.notices.clear();
                state.terminal = Some(RepositoryContextRetired {
                    lifetime_ids: Vec::new(),
                    sequence: state.sequence.to_string(),
                    all_retired: true,
                    terminal: true,
                });
            }
            state
                .leases
                .drain()
                .map(|(_, lease)| lease)
                .collect::<Vec<_>>()
        };
        self.notify.notify_waiters();
        self.parent.end_scope();
        self.review.close();
        self.resource.close();
        self.selection.close();
        self.origin.retire();
        for lease in leases {
            lease.lifetime.retirement().end_scope();
        }
    }
    fn retire_id(&self, id: &str) {
        let (lease, overflow) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let lease = state.leases.remove(id);
            let mut overflow = false;
            if lease.is_some() && !state.closed {
                if let Some(next) = state
                    .sequence
                    .checked_add(1)
                    .filter(|_| state.notices.len() < NOTICE_LIMIT)
                {
                    state.sequence = next;
                    state.notices.push_back(RepositoryContextRetired {
                        lifetime_ids: vec![id.to_owned()],
                        sequence: next.to_string(),
                        all_retired: false,
                        terminal: false,
                    });
                } else {
                    overflow = true;
                }
            }
            (lease, overflow)
        };
        if let Some(lease) = lease {
            lease.lifetime.retirement().end_scope();
        }
        if overflow {
            self.close();
        }
        self.notify.notify_waiters();
    }
    fn new_lifetime(&self) -> AdmissionResult<(RepositorySourceLifetime, RepositorySubscription)> {
        RepositorySourceLifetime::for_optional_request(
            self.services.repository_lifecycle_registry.clone(),
            self.origin.origin(),
            &self.parent,
        )
    }
}
impl Connection {
    fn capture_native(
        &self,
        query: Option<&RepositoryContextQuery>,
    ) -> Arc<dyn RepositoryReadRequestScope> {
        let captured = (|| {
            self.entered()?;
            if NATIVE_REQUEST.try_with(|_| ()).is_ok() {
                return Err(AdmissionError::Denied);
            }
            let (lifetime, registration) = self.new_lifetime()?;
            let mut subscriptions = Vec::new();
            if let Some(query) = query {
                let mut keys = vec![
                    RepositoryLifecycleKey::Database,
                    RepositoryLifecycleKey::WireAuthority,
                    RepositoryLifecycleKey::Workspace(query.workspace_id.clone()),
                    RepositoryLifecycleKey::Selection {
                        workspace_id: query.workspace_id.clone(),
                        git_root_id: query.git_root_id.clone(),
                    },
                ];
                if let Some(id) = &query.git_root_id {
                    keys.push(RepositoryLifecycleKey::GitRoot(id.clone()));
                } else {
                    keys.push(RepositoryLifecycleKey::RootInventory(
                        query.workspace_id.clone(),
                    ));
                }
                subscriptions.push(lifetime.subscribe(
                    &self.services.store,
                    self.caller.caller(),
                    &keys,
                )?);
            }
            Ok((lifetime, registration, subscriptions))
        })();
        let (lifetime, registration, subscriptions) = match captured {
            Ok((a, b, c)) => (Some(a), Some(b), c),
            Err(_) => (None, None, Vec::new()),
        };
        let request = Arc::new_cyclic(|weak| Request {
            connection: self.weak.upgrade().expect("live connection"),
            weak: weak.clone(),
            lifetime,
            _registration: registration,
            state: Mutex::new(RequestState::Ordinary),
            completed: AtomicBool::new(false),
            consumed: AtomicBool::new(false),
            read_lane: Mutex::new(None),
            subscriptions: Mutex::new(subscriptions),
            queued_query: query.cloned(),
        });
        if query.is_some() {
            let weak = Arc::downgrade(&request);
            let retirement = request
                .lifetime
                .as_ref()
                .map(RepositorySourceLifetime::retirement);
            tokio::spawn(async move {
                if let Some(retirement) = retirement {
                    tokio::select! { () = retirement.native_cancelled() => return, () = tokio::time::sleep(FRAME_TTL) => {} }
                } else {
                    return;
                }
                if let Some(request) = weak.upgrade() {
                    request.finish();
                }
            });
        }
        request
    }
}
impl RepositoryReadConnection for Connection {
    fn capture_resource(
        &self,
        frame: &intent_core::repository_request::RepositoryResourceFrame,
    ) -> Option<Arc<dyn RepositoryReadRequestScope>> {
        Some(resource::capture_frame(self, frame.clone()))
    }
    fn take_resource_retirements(
        &self,
    ) -> Option<Box<dyn intent_core::repository_request::RepositoryResourceRetirements>> {
        resource::take_retirements(self)
    }

    fn capture_review(
        &self,
        frame: &intent_core::repository_request::NativeReviewFrame,
    ) -> Option<Arc<dyn RepositoryReadRequestScope>> {
        Some(review::capture_frame(self, frame.clone()))
    }
    fn take_review_retirements(
        &self,
    ) -> Option<Box<dyn intent_core::repository_request::NativeReviewRetirements>> {
        review::take_retirements(self)
    }

    fn capture_selection(
        &self,
        frame: &intent_core::repository_request::RepositorySelectionFrame,
    ) -> Option<Arc<dyn RepositoryReadRequestScope>> {
        Some(selection::capture_frame(self, frame.clone()))
    }
    fn take_selection_retirements(
        &self,
    ) -> Option<Box<dyn intent_core::repository_request::RepositorySelectionRetirements>> {
        selection::take_retirements(self)
    }
    fn capture(&self) -> Arc<dyn RepositoryReadRequestScope> {
        self.capture_native(None)
    }
    fn capture_context(
        &self,
        query: &RepositoryContextQuery,
    ) -> Arc<dyn RepositoryReadRequestScope> {
        self.capture_native(Some(query))
    }
    fn retire(&self) {
        self.close();
    }
    fn take_retirements(&self) -> Option<Box<dyn RepositoryReadRetirements>> {
        let mut state = self.state.lock().ok()?;
        if state.feed_taken || state.closed {
            return None;
        }
        state.feed_taken = true;
        Some(Box::new(Receiver {
            connection: self.weak.clone(),
            events: self.services.event_bus.as_ref().map(|bus| {
                bus.subscribe(crate::SubscriptionFilter {
                    event_types: vec![
                        "git:*".into(),
                        "changes:git-status".into(),
                        "sourceControl:auth-changed".into(),
                    ],
                    ..Default::default()
                })
            }),
            settings: self
                .services
                .settings_registry
                .as_ref()
                .map(|registry| registry.subscribe()),
        }))
    }
}
struct Receiver {
    connection: Weak<Connection>,
    events: Option<crate::Subscription>,
    settings: Option<tokio::sync::watch::Receiver<crate::settings_registry::SettingsChanged>>,
}
impl Drop for Receiver {
    fn drop(&mut self) {
        if let Some(c) = self.connection.upgrade() {
            c.close();
        }
    }
}
impl RepositoryReadRetirements for Receiver {
    fn next(&mut self) -> BoxFuture<'_, Option<RepositoryContextRetired>> {
        Box::pin(async move {
            let c = self.connection.upgrade()?;
            loop {
                let wake = c.notify.notified();
                tokio::pin!(wake);
                wake.as_mut().enable();
                {
                    let mut state = c
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if let Some(terminal) = state.terminal.take() {
                        return Some(terminal);
                    }
                    if state.closed {
                        return None;
                    }
                    if let Some(notice) = state.notices.pop_front() {
                        return Some(notice);
                    }
                }
                tokio::select! {
                    () = wake => {},
                    event = async { match &mut self.events { Some(events) => events.recv_delivery().await, None => std::future::pending().await } } => {
                        match event {
                            Some(crate::Delivery::Batch(events)) => {
                                let ids = {
                                    let state = c.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                                    state.leases.values().filter(|lease| events.iter().any(|event| {
                                        (event.event_type == "sourceControl:auth-changed" && lease.provider.is_some()) ||
                                        ((event.event_type.starts_with("git:") || event.event_type == "changes:git-status") && event.workspace_id == lease.query.workspace_id)
                                    })).map(|lease| lease.id.clone()).collect::<Vec<_>>()
                                };
                                for id in ids { c.retire_id(&id); }
                            }
                            Some(crate::Delivery::Lagged(_)) | None => c.close(),
                        }
                    },
                    changed = async { match &mut self.settings { Some(settings) => settings.changed().await, None => std::future::pending().await } } => {
                        if changed.is_err() { c.close(); } else {
                            let ids = c.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner).leases.values().filter(|lease|lease.settings.is_some()).map(|lease|lease.id.clone()).collect::<Vec<_>>();
                            for id in ids { c.retire_id(&id); }
                        }
                    },
                }
            }
        })
    }
}

struct Request {
    connection: Arc<Connection>,
    weak: Weak<Self>,
    lifetime: Option<RepositorySourceLifetime>,
    _registration: Option<RepositorySubscription>,
    state: Mutex<RequestState>,
    completed: AtomicBool,
    consumed: AtomicBool,
    read_lane: Mutex<Option<tokio::sync::OwnedMutexGuard<()>>>,
    subscriptions: Mutex<Vec<RepositorySubscription>>,
    queued_query: Option<RepositoryContextQuery>,
}
#[derive(Clone)]
enum RequestState {
    Ordinary,
    Pending,
    Public,
    Capture(Arc<Lease>),
    Reading(Arc<Lease>),
    Read(Arc<Lease>, Arc<Observation>),
}
impl Request {
    fn current(services: &Services) -> AdmissionResult<Arc<Self>> {
        let request = NATIVE_REQUEST.try_with(Clone::clone).map_err(local)?;
        if !std::ptr::eq(request.connection.services.as_ref(), services) {
            return Err(AdmissionError::Denied);
        }
        request.check()?;
        let mut state = request.state.lock().map_err(local)?;
        if !matches!(*state, RequestState::Ordinary) {
            return Err(AdmissionError::Denied);
        }
        *state = RequestState::Pending;
        drop(state);
        Ok(request)
    }
    fn matches_query(&self, query: &RepositoryContextQuery) -> AdmissionResult<()> {
        if self.queued_query.as_ref().is_some_and(|old| old != query) {
            Err(AdmissionError::Denied)
        } else {
            Ok(())
        }
    }
    fn check(&self) -> AdmissionResult<()> {
        if self.completed.load(Ordering::Acquire) {
            return Err(AdmissionError::Retired);
        }
        self.connection.entered()?;
        self.lifetime
            .as_ref()
            .ok_or(AdmissionError::Unavailable)?
            .retirement()
            .check_current()
    }
    fn subscribe(&self, keys: &[RepositoryLifecycleKey]) -> AdmissionResult<()> {
        let subscription = self
            .lifetime
            .as_ref()
            .ok_or(AdmissionError::Unavailable)?
            .subscribe(
                &self.connection.services.store,
                self.connection.caller.caller(),
                keys,
            )?;
        self.subscriptions.lock().map_err(local)?.push(subscription);
        Ok(())
    }
    fn finish(&self) {
        self.completed.store(true, Ordering::Release);
        if let Some(lifetime) = &self.lifetime {
            lifetime.retirement().end_scope();
        }
        self.read_lane
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        match state {
            RequestState::Capture(lease) if !lease.published.load(Ordering::Acquire) => {
                self.connection.retire_id(&lease.id);
            }
            RequestState::Reading(lease) | RequestState::Read(lease, _)
                if !self.consumed.load(Ordering::Acquire) =>
            {
                self.connection.retire_id(&lease.id);
            }
            _ => {}
        }
    }
}
impl Drop for Request {
    fn drop(&mut self) {
        self.finish();
    }
}
impl RepositoryReadRequestScope for Request {
    fn scope<'a>(&'a self, body: BoxFuture<'a, ()>) -> BoxFuture<'a, ()> {
        let owner = self.weak.upgrade().expect("owned request scope");
        Box::pin(NATIVE_REQUEST.scope(owner, body))
    }
    fn retire(&self) {
        self.finish();
    }
    fn deliver<'a>(
        &'a self,
        _kind: RepositoryReadReplyKind,
        transfer: &'a mut (dyn FnMut() -> Result<()> + Send),
    ) -> BoxFuture<'a, Result<()>> {
        let entered = self.check();
        Box::pin(async move {
            let state = self.state.lock().map_err(|_| unavailable())?.clone();
            match state {
                RequestState::Ordinary | RequestState::Public => transfer(),
                RequestState::Pending | RequestState::Reading(_) => Err(unavailable()),
                RequestState::Capture(lease) => {
                    entered.map_err(|_| unavailable())?;
                    self.deliver_lease(&lease, None, transfer).await
                }
                RequestState::Read(lease, observation) => {
                    entered.map_err(|_| unavailable())?;
                    self.deliver_lease(&lease, Some(&observation), transfer)
                        .await
                }
            }
        })
    }
}

struct Lease {
    id: String,
    query: RepositoryContextQuery,
    scope: ExecutionScope,
    deadline: Instant,
    lifetime: RepositorySourceLifetime,
    _registration: RepositorySubscription,
    _subscriptions: Vec<RepositorySubscription>,
    _permit: OwnedSemaphorePermit,
    authority: RepositoryAuthoritySnapshot,
    roots: Vec<(RootRecord, SelectionFacts)>,
    settings: Option<(Arc<SettingsRegistry>, Arc<SettingsSnapshot>)>,
    provider: Option<Arc<RepositoryConnectionFacts>>,
    // Private target qualification never implies administrator disclosure.
    administrator_projection: bool,
    state: Mutex<Revision>,
    lane: Arc<tokio::sync::Mutex<()>>,
    reads: AtomicUsize,
    published: AtomicBool,
}
#[derive(Default)]
struct Revision {
    sequence: u64,
    observed: Option<Arc<Observation>>,
}
#[derive(Clone, PartialEq, Eq)]
struct Observation {
    roots: Vec<(RepositoryRootContext, RepositoryObservedRoot)>,
}
impl Lease {
    fn check(&self) -> AdmissionResult<()> {
        if Instant::now() >= self.deadline {
            return Err(AdmissionError::Retired);
        }
        self.lifetime.retirement().check_current()
    }
    fn with_metadata<T: Send>(
        &self,
        action: impl FnOnce() -> AdmissionResult<T> + Send,
    ) -> AdmissionResult<T> {
        let batch = || {
            let mut out = None;
            RepositoryConnectionFacts::with_native_context_current(
                self.provider.as_deref(),
                |current| {
                    if self.provider.is_none() || current {
                        out = Some(action());
                    } else {
                        tracing::debug!(
                            phase = "provider comparison",
                            "original repository context refused"
                        );
                    }
                    Ok(())
                },
            )
            .map_err(local)?;
            out.ok_or(AdmissionError::Retired)?
        };
        match &self.settings {
            Some((registry, settings)) => registry.with_original_snapshot(settings, |current| {
                if current {
                    batch()
                } else {
                    tracing::debug!(
                        phase = "settings comparison",
                        "original repository context refused"
                    );
                    Err(AdmissionError::Retired)
                }
            }),
            None => batch(),
        }
    }
}

async fn authority(
    request: &Request,
    workspace: &intent_core::WorkspaceId,
) -> AdmissionResult<RepositoryAuthoritySnapshot> {
    request.check()?;
    let c = &request.connection;
    let Caller::Wire {
        principal_id,
        host_role,
    } = c.caller.caller()
    else {
        return Err(AdmissionError::Denied);
    };
    let hash = match c.caller.wire_credential() {
        Some(WireCredential::Principal { token_hash, .. }) => Some(token_hash.as_str()),
        _ => None,
    };
    let before = c
        .services
        .store
        .repository_authority_snapshot(workspace, principal_id, hash)
        .await
        .map_err(local)?;
    if before.workspace.value.is_none() || before.principal.value.is_none() {
        return Err(AdmissionError::Denied);
    }
    c.services.require_member(workspace).await.map_err(local)?;
    if &c
        .services
        .store
        .get_host_role(principal_id)
        .await
        .map_err(local)?
        != host_role
    {
        return Err(AdmissionError::Denied);
    }
    let credential = if let Some(hash) = hash {
        c.services
            .store
            .lookup_principal_credential(hash)
            .await
            .map_err(local)?
    } else {
        None
    };
    let after = c
        .services
        .store
        .repository_authority_snapshot(workspace, principal_id, hash)
        .await
        .map_err(local)?;
    if before != after {
        return Err(AdmissionError::Retired);
    }
    let facts = RepositoryAuthorityFacts {
        caller: c.caller.caller().clone(),
        workspace: workspace.clone(),
        workspace_exists: true,
        primary_principal_id: before
            .primary_principal
            .value
            .as_ref()
            .map(|p| p.id.clone()),
        workspace_role: before.workspace_grant.value,
        credential,
        provenance: RepositoryAuthorityProvenance::Store(Box::new(before.clone())),
        internal_stages: Vec::new(),
    };
    c.caller.verify(&facts, workspace)?;
    request.check()?;
    Ok(before)
}

fn query_of(q: &RepositoryContextBoundQuery) -> RepositoryContextQuery {
    RepositoryContextQuery {
        workspace_id: q.workspace_id.clone(),
        git_root_id: q.git_root_id.clone(),
    }
}
fn find_lease(request: &Request, q: &RepositoryContextBoundQuery) -> AdmissionResult<Arc<Lease>> {
    request.check()?;
    request.matches_query(&query_of(q))?;
    let lease = request
        .connection
        .state
        .lock()
        .map_err(local)?
        .leases
        .get(&q.repository_lifetime_id)
        .cloned()
        .ok_or(AdmissionError::Denied)?;
    if lease.query != query_of(q) || !lease.published.load(Ordering::Acquire) {
        return Err(AdmissionError::Denied);
    }
    lease.check()?;
    Ok(lease)
}

pub(crate) fn capture(
    services: &Services,
    query: RepositoryContextQuery,
) -> BoxFuture<'_, Result<RepositoryContextCapture>> {
    let request = Request::current(services);
    Box::pin(async move {
        let request = request.map_err(|_| unavailable())?;
        tokio::time::timeout(ACQUIRE_TIMEOUT, capture_inner(&request, query))
            .await
            .map_err(|_| unavailable())?
            .map_err(|_| unavailable())
    })
}
async fn capture_inner(
    request: &Arc<Request>,
    query: RepositoryContextQuery,
) -> AdmissionResult<RepositoryContextCapture> {
    request.check()?;
    request.matches_query(&query)?;
    let c = &request.connection;
    let permit = c.permits.clone().try_acquire_owned().map_err(local)?;
    let (lifetime, registration) = c.new_lifetime()?;
    let mut subscriptions = vec![lifetime.subscribe(
        &c.services.store,
        c.caller.caller(),
        &[
            RepositoryLifecycleKey::Database,
            RepositoryLifecycleKey::WireAuthority,
            RepositoryLifecycleKey::Workspace(query.workspace_id.clone()),
        ],
    )?];
    request.subscribe(&[
        RepositoryLifecycleKey::Database,
        RepositoryLifecycleKey::WireAuthority,
        RepositoryLifecycleKey::Workspace(query.workspace_id.clone()),
    ])?;
    let _credential = c.caller.legacy_lease().await?;
    let initial = authority(request, &query.workspace_id).await?;
    let mut roots = Vec::new();
    let coverage = if let Some(id) = &query.git_root_id {
        roots.push(RepositoryRootId {
            workspace_id: query.workspace_id.clone(),
            kind: RepositoryRootKind::Registered {
                git_root_id: id.clone(),
            },
        });
        RepositoryContextCoverage::RegisteredRoot {
            workspace_id: query.workspace_id.clone(),
            git_root_id: id.clone(),
        }
    } else {
        subscriptions.push(lifetime.subscribe(
            &c.services.store,
            c.caller.caller(),
            &[
                RepositoryLifecycleKey::Database,
                RepositoryLifecycleKey::RootInventory(query.workspace_id.clone()),
            ],
        )?);
        roots.push(RepositoryRootId {
            workspace_id: query.workspace_id.clone(),
            kind: RepositoryRootKind::Primary,
        });
        let registered = c
            .services
            .store
            .list_workspace_git_roots(&query.workspace_id)
            .await
            .map_err(local)?;
        if registered.len() >= ROOT_LIMIT {
            return Err(AdmissionError::Unavailable);
        }
        for root in registered {
            roots.push(RepositoryRootId {
                workspace_id: query.workspace_id.clone(),
                kind: RepositoryRootKind::Registered {
                    git_root_id: root.id,
                },
            });
        }
        roots[1..].sort_by_key(|r| match &r.kind {
            RepositoryRootKind::Registered { git_root_id } => git_root_id.to_string(),
            RepositoryRootKind::Primary => String::new(),
        });
        RepositoryContextCoverage::WorkspaceInventory {
            workspace_id: query.workspace_id.clone(),
        }
    };
    let mut records = Vec::new();
    for root in roots {
        let mut keys = vec![RepositoryLifecycleKey::Database, SelectionFacts::key(&root)];
        if let RepositoryRootKind::Registered { git_root_id } = &root.kind {
            keys.push(RepositoryLifecycleKey::GitRoot(git_root_id.clone()));
        }
        subscriptions.push(lifetime.subscribe(&c.services.store, c.caller.caller(), &keys)?);
        let record = RootRecord::read(&c.services.store, &root).await?;
        subscriptions.push(lifetime.subscribe(
            &c.services.store,
            c.caller.caller(),
            &[RepositoryLifecycleKey::Database, record.lifecycle_key()],
        )?);
        records.push((record, SelectionFacts::read(&c.services, &root).await?));
    }
    let admin = Services::require_administrator("sourceControl.authStatus").is_ok();
    let member = matches!(
        c.caller.caller(),
        Caller::Wire {
            host_role: intent_core::HostRole::Member,
            ..
        }
    );
    if member {
        c.services
            .require_host_execution("Repository context")
            .await
            .map_err(local)?;
    }
    let svc = c.services.clone();
    let job_permit = c.workers.clone().try_acquire_owned().map_err(local)?;
    c.active_jobs.fetch_add(1, Ordering::AcqRel);
    let job = JobGuard {
        connection: c.clone(),
        _permit: job_permit,
    };
    let (settings, provider) = tokio::task::spawn_blocking(move || {
        let _job = job;
        let settings = svc
            .settings_registry
            .as_ref()
            .map(|registry| (registry.clone(), registry.snapshot()));
        let provider = (admin || member)
            .then(|| svc.gitlab_repository_connection_facts().ok())
            .flatten()
            .filter(|f| {
                !matches!(
                    f.attachment(),
                    RepositoryAttachmentState::Unattached
                        | RepositoryAttachmentState::BoundaryMissing
                )
            })
            .map(Arc::new);
        (settings, provider)
    })
    .await
    .map_err(local)?;
    if authority(request, &query.workspace_id).await? != initial {
        return Err(AdmissionError::Retired);
    }
    lifetime.retirement().check_current()?;
    let id = uuid::Uuid::new_v4().to_string();
    let scope = ExecutionScope {
        daemon_id: c.services.daemon_boot_id.clone(),
        authority_scope_id: id.clone(),
        authority_generation: initial
            .workspace
            .revision
            .ok_or(AdmissionError::Unavailable)?
            .get(),
    };
    let lease = Arc::new(Lease {
        id: id.clone(),
        query,
        scope: scope.clone(),
        deadline: Instant::now() + LEASE_TTL,
        lifetime,
        _registration: registration,
        _subscriptions: subscriptions,
        _permit: permit,
        authority: initial,
        roots: records,
        settings,
        administrator_projection: admin,
        provider,
        state: Mutex::new(Revision::default()),
        lane: Arc::new(tokio::sync::Mutex::new(())),
        reads: AtomicUsize::new(0),
        published: AtomicBool::new(false),
    });
    *request.state.lock().map_err(local)? = RequestState::Capture(lease.clone());
    let sequence = {
        let mut state = c.state.lock().map_err(local)?;
        if state.closed {
            return Err(AdmissionError::Retired);
        }
        state.leases.insert(id.clone(), lease.clone());
        state.sequence
    };
    let retirement = lease.lifetime.retirement();
    let weak = c.weak.clone();
    let expiry = tokio::time::Instant::from_std(lease.deadline);
    let watched = id.clone();
    tokio::spawn(async move {
        tokio::select! { () = retirement.native_cancelled() => {}, () = tokio::time::sleep_until(expiry) => {} }
        if let Some(c) = weak.upgrade() {
            c.retire_id(&watched);
        }
    });
    Ok(RepositoryContextCapture {
        lifetime_id: id,
        scope,
        coverage,
        retirement_sequence: sequence.to_string(),
        expires_after_ms: LEASE_TTL.as_millis().try_into().map_err(local)?,
    })
}

pub(crate) fn read(
    services: &Services,
    query: RepositoryContextBoundQuery,
) -> BoxFuture<'_, Result<RepositoryContext>> {
    let request = Request::current(services);
    Box::pin(async move {
        let request = request.map_err(|_| unavailable())?;
        let lease = find_lease(&request, &query).map_err(|_| unavailable())?;
        *request.state.lock().map_err(|_| unavailable())? = RequestState::Reading(lease.clone());
        let result = tokio::time::timeout(READ_TIMEOUT, read_inner(&request, &lease))
            .await
            .map_err(|_| AdmissionError::Unavailable)
            .and_then(|r| r);
        if result.is_err() {
            request.connection.retire_id(&lease.id);
        }
        result
            .inspect_err(|error| {
                tracing::debug!(
                    ?error,
                    phase = "read",
                    "original repository context refused"
                );
            })
            .map_err(|_| unavailable())
    })
}
async fn read_inner(
    request: &Arc<Request>,
    lease: &Arc<Lease>,
) -> AdmissionResult<RepositoryContext> {
    let lane = lease.lane.clone().try_lock_owned().map_err(local)?;
    *request.read_lane.lock().map_err(local)? = Some(lane);
    lease
        .reads
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
            n.checked_add(1).filter(|n| *n <= READ_LIMIT)
        })
        .map_err(local)?;
    let _credential = request.connection.caller.legacy_lease().await?;
    validate(request, lease).await?;
    let (observed, _locks) = observe_locked(request.clone(), lease.clone()).await?;
    request.check()?;
    lease.check()?;
    let mut revision = lease.state.lock().map_err(local)?;
    if revision
        .observed
        .as_ref()
        .is_some_and(|old| old.as_ref() != &observed)
    {
        return Err(AdmissionError::BindingChanged);
    }
    if revision.observed.is_none() {
        revision.sequence = revision
            .sequence
            .checked_add(1)
            .ok_or(AdmissionError::Unavailable)?;
        revision.observed = Some(Arc::new(observed));
    }
    let observation = revision
        .observed
        .as_ref()
        .ok_or(AdmissionError::Unavailable)?
        .clone();
    let context = RepositoryContext {
        scope: lease.scope.clone(),
        revision: RepositoryContextRevision::new(&lease.id, revision.sequence),
        roots: observation.roots.iter().map(|r| r.0.clone()).collect(),
    };
    *request.state.lock().map_err(local)? = RequestState::Read(lease.clone(), observation);
    Ok(context)
}
pub(crate) fn release(
    services: &Services,
    query: RepositoryContextBoundQuery,
) -> BoxFuture<'_, Result<RepositoryContextReleased>> {
    let request = Request::current(services);
    Box::pin(async move {
        let request = request.map_err(|_| unavailable())?;
        request.check().map_err(|_| unavailable())?;
        request
            .matches_query(&query_of(&query))
            .map_err(|_| unavailable())?;
        let old = request
            .connection
            .state
            .lock()
            .map_err(|_| unavailable())?
            .leases
            .get(&query.repository_lifetime_id)
            .cloned();
        if let Some(old) = old {
            if old.query != query_of(&query) {
                return Err(unavailable());
            }
            request.connection.retire_id(&old.id);
        }
        *request.state.lock().map_err(|_| unavailable())? = RequestState::Public;
        Ok(RepositoryContextReleased { released: true })
    })
}

async fn validate(request: &Request, lease: &Lease) -> AdmissionResult<()> {
    request.check()?;
    lease.check()?;
    if authority(request, &lease.query.workspace_id).await? != lease.authority {
        return Err(AdmissionError::Retired);
    }
    if lease.administrator_projection {
        Services::require_administrator("sourceControl.authStatus").map_err(local)?;
    } else if lease.provider.is_some() {
        request
            .connection
            .services
            .require_host_execution("Repository context")
            .await
            .map_err(local)?;
    }
    for (record, selection) in &lease.roots {
        if RootRecord::read(&request.connection.services.store, record.root()).await? != *record
            || SelectionFacts::read(&request.connection.services, record.root()).await?
                != *selection
        {
            return Err(AdmissionError::BindingChanged);
        }
    }
    request.check()?;
    lease.check()
}

struct JobGuard {
    connection: Arc<Connection>,
    _permit: OwnedSemaphorePermit,
}
impl Drop for JobGuard {
    fn drop(&mut self) {
        self.connection.active_jobs.fetch_sub(1, Ordering::AcqRel);
        self.connection.jobs_changed.notify_waiters();
    }
}
struct Locks {
    _release: oneshot::Sender<()>,
}
fn project_target_context(
    target: &intent_core::RepositoryTarget,
    facts: Option<&RepositoryConnectionFacts>,
    administrator_projection: bool,
    member_projection: bool,
) -> intent_core::RepositoryTargetContext {
    if administrator_projection {
        return target_context(target, facts);
    }
    let mut context = target_context(target, None);
    // An original host Member may observe this exact settled connection without
    // its administrator identities. This is neither provider reachability nor
    // permission, capability, execution admission, or a reservation. Capabilities
    // stay Unknown; the original lease and consuming transfer still compare P/R.
    if member_projection
        && target.provider == intent_core::RepositoryProvider::Gitlab
        && facts.is_some_and(|facts| {
            facts.attachment() == RepositoryAttachmentState::Paired
                && facts.approval() == RepositoryDescriptorState::Approved
                && facts.settled().is_some_and(|settled| {
                    settled.descriptor().instance().as_str() == target.instance_base_url
                })
        })
    {
        context.availability = intent_core::RepositoryAvailability::Connected;
    }
    context
}

async fn observe_locked(
    request: Arc<Request>,
    lease: Arc<Lease>,
) -> AdmissionResult<(Observation, Locks)> {
    let c = request.connection.clone();
    let permit = c.workers.clone().try_acquire_owned().map_err(local)?;
    c.active_jobs.fetch_add(1, Ordering::AcqRel);
    let job = JobGuard {
        connection: c.clone(),
        _permit: permit,
    };
    let (ready, result) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let caller = c.caller.caller().clone();
    let credential = c.caller.wire_credential().cloned();
    tokio::spawn(with_caller(
        caller,
        with_wire_credential(credential, async move {
            let _job = job;
            let mut ready = Some(ready);
            let svc = c.services.clone();
            let acquired = AtomicBool::new(false);
            let request_stop = request
                .lifetime
                .as_ref()
                .expect("qualified request")
                .retirement();
            let lease_stop = lease.lifetime.retirement();
            let outcome = {
                let work = RepositoryGitSource::with_mixed_group(
                    &svc.store,
                    &svc.worktree_locks,
                    lease.roots.iter().map(|r| r.0.clone()).collect(),
                    Vec::new(),
                    RepositoryRetirement::default(),
                    |sources, _| async {
                        acquired.store(true, Ordering::Release);
                        validate(&request, &lease).await?;
                        let mut roots = Vec::new();
                        for ((record, selection), source) in lease.roots.iter().zip(sources) {
                            source.check_root().await?;
                            let id = record.root().clone();
                            let path = record.path().to_owned();
                            let saved = selection.saved()?;
                            let facts = lease.provider.clone();
                            let administrator_projection = lease.administrator_projection;
                            let member_projection = matches!(
                                c.caller.caller(),
                                Caller::Wire {
                                    host_role: intent_core::HostRole::Member,
                                    ..
                                }
                            );
                            let resolve = resolver(facts.as_deref())?;
                            #[cfg(test)]
                            let probe = c.git_probe.lock().unwrap().clone();
                            let value = tokio::task::spawn_blocking(move || {
                                #[cfg(test)]
                                if let Some(probe) = probe {
                                    probe();
                                }
                                read_context_root_with_resolver(
                                    &id,
                                    &path,
                                    &saved,
                                    None,
                                    &resolve,
                                    &GitConfigEnvironment::default(),
                                    |target| {
                                        project_target_context(
                                            target,
                                            facts.as_deref(),
                                            administrator_projection,
                                            member_projection,
                                        )
                                    },
                                )
                            })
                            .await
                            .map_err(local)?
                            .map_err(local)?;
                            source.check_root().await?;
                            roots.push(value);
                        }
                        validate(&request, &lease).await?;
                        if ready
                            .take()
                            .expect("one worker result")
                            .send(Ok(Observation { roots }))
                            .is_ok()
                        {
                            let _ = released.await;
                        }
                        Ok(())
                    },
                );
                tokio::pin!(work);
                tokio::select! {
                    result = &mut work => result,
                    () = request_stop.native_cancelled() => if acquired.load(Ordering::Acquire) { work.await } else { Err(AdmissionError::Retired) },
                    () = lease_stop.native_cancelled() => if acquired.load(Ordering::Acquire) { work.await } else { Err(AdmissionError::Retired) },
                }
            };
            if let Some(ready) = ready {
                let _ = ready.send(Err(outcome.err().unwrap_or(AdmissionError::Unavailable)));
            }
        }),
    ));
    let locks = Locks { _release: release };
    let observation = result.await.map_err(local)??;
    Ok((observation, locks))
}

impl Request {
    async fn deliver_lease(
        &self,
        lease: &Arc<Lease>,
        observed: Option<&Arc<Observation>>,
        transfer: &mut (dyn FnMut() -> Result<()> + Send),
    ) -> Result<()> {
        let result = tokio::time::timeout(READ_TIMEOUT, async {
            let _credential = self.connection.caller.legacy_lease().await?;
            validate(self, lease).await?;
            let locks = if let Some(original) = observed {
                let (fresh, locks) = observe_locked(
                    self.weak.upgrade().ok_or(AdmissionError::Retired)?,
                    lease.clone(),
                )
                .await?;
                if &fresh != original.as_ref() {
                    return Err(AdmissionError::BindingChanged);
                }
                Some(locks)
            } else {
                None
            };
            // A future moved between polls cannot borrow the worker's restored
            // caller to admit output under a foreign surrounding entry.
            self.check()?;
            lease.check()?;
            self.connection
                .parent
                .native_dispatch(|| {
                    self.lifetime
                        .as_ref()
                        .ok_or(AdmissionError::Unavailable)?
                        .retirement()
                        .native_dispatch(|| {
                            lease.lifetime.retirement().native_dispatch(|| {
                                if Instant::now() >= lease.deadline {
                                    return Err(AdmissionError::Retired);
                                }
                                let revision = lease.state.lock().map_err(local)?;
                                if let Some(original) = observed {
                                    if !revision
                                        .observed
                                        .as_ref()
                                        .is_some_and(|current| Arc::ptr_eq(current, original))
                                    {
                                        return Err(AdmissionError::Retired);
                                    }
                                }
                                lease.with_metadata(|| {
                                    if self.consumed.swap(true, Ordering::AcqRel) {
                                        return Err(AdmissionError::Retired);
                                    }
                                    let result = transfer();
                                    if result.is_ok() {
                                        lease.published.store(true, Ordering::Release);
                                    }
                                    Ok(result)
                                })
                            })
                        })
                })
                .inspect(|_| {
                    drop(locks);
                })
        })
        .await
        .map_err(|_| AdmissionError::Unavailable)
        .and_then(|r| r);
        if result.is_err() {
            tracing::debug!(request = ?self.check(), lease = ?lease.check(), phase = "final lifetime", "original repository context refused");
            self.connection.retire_id(&lease.id);
        }
        result
            .inspect_err(|error| {
                tracing::debug!(
                    ?error,
                    phase = "delivery",
                    "original repository context refused"
                );
            })
            .map_err(|_| unavailable())?
    }
}

#[cfg(test)]
#[path = "native_wire/tests.rs"]
mod tests;
