//! Original native socket selection intent. Read leases never authorize writes.
//! An admitted Store future owns its worker and receipt beyond a waiting frame.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::time::Instant;

use intent_core::caller::{with_caller, with_wire_credential, WireCredential};
use intent_core::repository_request::{
    RepositoryReadReplyKind, RepositoryReadRequestScope, RepositorySelectionAttempt as Attempt,
    RepositorySelectionAttemptState as AttemptState, RepositorySelectionBoundQuery as BoundQuery,
    RepositorySelectionCapture as Capture, RepositorySelectionChoice as Choice,
    RepositorySelectionFailure as Failure, RepositorySelectionFrame as Frame,
    RepositorySelectionPersistence as Persistence, RepositorySelectionQuery as Query,
    RepositorySelectionReceipt as Receipt, RepositorySelectionReleased as Released,
    RepositorySelectionResult as Outcome, RepositorySelectionRetired as Retired,
    RepositorySelectionRetirements, RepositorySelectionSaveQuery as SaveQuery,
    RepositorySelectionSnapshot as Snapshot, RepositorySelectionState as SelectionState,
};
use intent_core::{
    BoxFuture, Caller, Error, ExecutionScope, RepositoryRootId, RepositoryRootKind, Result,
};
use intent_store::{
    RepositoryAuthoritySnapshot, RepositoryLifecycleKey, RepositorySelectionChange as Change,
    RepositorySelectionPersistence as StorePersistence,
    RepositorySelectionSnapshot as StoreSnapshot, RepositorySelectionWriteOutcome,
    RepositorySelectionWriteResult, RepositoryStoredSelection,
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use super::{Connection, Services};
use crate::repository_admission::lifecycle::{RepositorySourceLifetime, RepositorySubscription};
use crate::repository_admission::{
    AdmissionError, RepositoryAuthorityFacts, RepositoryAuthorityProvenance,
};

const CONNECTION_LIMIT: usize = 64;
const GLOBAL_LIMIT: usize = 256;
const WORKER_LIMIT: usize = 2;
const RECONCILE_LIMIT: usize = 64;
const NOTICE_LIMIT: usize = 64;
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);
const FRAME_TTL: Duration = Duration::from_secs(15);
const ADMISSION_TIMEOUT: Duration = Duration::from_secs(10);
const LEASE_TTL: Duration = Duration::from_secs(300);
const RECEIPT_TTL: Duration = Duration::from_secs(300);

fn unavailable() -> Error {
    Error::Forbidden("Repository selection unavailable".into())
}
fn denied(_: impl std::fmt::Debug) -> Error {
    unavailable()
}
fn changed_command() -> Error {
    Error::InvalidParams("Selection command already claimed".into())
}

pub(crate) struct Capacity {
    records: Arc<Semaphore>,
    workers: Arc<Semaphore>,
}
impl Default for Capacity {
    fn default() -> Self {
        Self {
            records: Arc::new(Semaphore::new(GLOBAL_LIMIT)),
            workers: Arc::new(Semaphore::new(WORKER_LIMIT)),
        }
    }
}
#[derive(Default)]
struct Feed {
    taken: bool,
    closed: bool,
    sequence: u64,
    terminal: Option<Retired>,
    notices: VecDeque<Retired>,
    records: HashMap<String, Arc<Operation>>,
}
pub(super) struct ConnectionState {
    #[cfg(test)]
    after_store: Mutex<Option<Arc<tests::WorkerGate>>>,
    feed: Mutex<Feed>,
    notify: Notify,
    permits: Arc<Semaphore>,
}
impl Default for ConnectionState {
    fn default() -> Self {
        Self {
            #[cfg(test)]
            after_store: Mutex::new(None),
            feed: Mutex::default(),
            notify: Notify::new(),
            permits: Arc::new(Semaphore::new(CONNECTION_LIMIT)),
        }
    }
}
impl ConnectionState {
    pub(super) fn close(&self) {
        let records = {
            let mut feed = self
                .feed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if feed.closed {
                return;
            }
            feed.closed = true;
            feed.sequence = feed.sequence.checked_add(1).unwrap_or(feed.sequence);
            feed.notices.clear();
            feed.terminal = Some(Retired {
                selection_ids: Vec::new(),
                sequence: feed.sequence.to_string(),
                all_retired: true,
                terminal: true,
            });
            feed.records.values().cloned().collect::<Vec<_>>()
        };
        self.notify.notify_waiters();
        for op in records {
            op.write.retirement().end_scope();
            op.disclosure.retirement().end_scope();
            op.changed.notify_waiters();
        }
    }
    fn check(&self) -> Result<()> {
        let feed = self.feed.lock().map_err(denied)?;
        if !feed.taken || feed.closed {
            Err(unavailable())
        } else {
            Ok(())
        }
    }
    fn notice(&self, id: &str) {
        let overflow = {
            let mut feed = self
                .feed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if feed.closed {
                return;
            }
            match feed.sequence.checked_add(1) {
                Some(sequence) if feed.notices.len() < NOTICE_LIMIT => {
                    feed.sequence = sequence;
                    feed.notices.push_back(Retired {
                        selection_ids: vec![id.into()],
                        sequence: sequence.to_string(),
                        all_retired: false,
                        terminal: false,
                    });
                    false
                }
                _ => true,
            }
        };
        if overflow {
            self.close();
        }
        self.notify.notify_waiters();
    }
}
struct Receiver {
    connection: Weak<Connection>,
}
impl Drop for Receiver {
    fn drop(&mut self) {
        if let Some(c) = self.connection.upgrade() {
            c.selection.close();
        }
    }
}
impl RepositorySelectionRetirements for Receiver {
    fn next(&mut self) -> BoxFuture<'_, Option<Retired>> {
        Box::pin(async move {
            let c = self.connection.upgrade()?;
            loop {
                let wake = c.selection.notify.notified();
                tokio::pin!(wake);
                wake.as_mut().enable();
                {
                    let mut feed = c
                        .selection
                        .feed
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if let Some(terminal) = feed.terminal.take() {
                        return Some(terminal);
                    }
                    if feed.closed {
                        return None;
                    }
                    if let Some(notice) = feed.notices.pop_front() {
                        return Some(notice);
                    }
                }
                wake.await;
            }
        })
    }
}
pub(super) fn take_retirements(c: &Connection) -> Option<Box<dyn RepositorySelectionRetirements>> {
    let mut feed = c.selection.feed.lock().ok()?;
    if feed.taken || feed.closed {
        return None;
    }
    feed.taken = true;
    Some(Box::new(Receiver {
        connection: c.weak.clone(),
    }))
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Command {
    Save(Choice),
    Reset,
}
impl Command {
    fn change(&self) -> Change {
        match self {
            Self::Save(Choice::Automatic {}) => Change::Automatic,
            Self::Save(Choice::ExplicitRemote { remote_name }) => Change::ExplicitRemote {
                remote_name: remote_name.clone(),
            },
            Self::Reset => Change::Reset,
        }
    }
    fn valid(&self) -> bool {
        match self {
            Self::Save(Choice::ExplicitRemote { remote_name }) => {
                !remote_name.is_empty()
                    && remote_name.len() <= 1024
                    && remote_name.trim() == remote_name
                    && !remote_name.chars().any(char::is_control)
            }
            _ => true,
        }
    }
}
#[derive(Default)]
struct Progress {
    command: Option<Command>,
    worker_started: bool,
    admitted: bool,
    receipt: Option<Receipt>,
    settled: Option<Instant>,
}
struct Operation {
    id: String,
    connection: Weak<Connection>,
    query: Query,
    snapshot: StoreSnapshot,
    authority: RepositoryAuthoritySnapshot,
    write: RepositorySourceLifetime,
    disclosure: RepositorySourceLifetime,
    _subscriptions: Vec<RepositorySubscription>,
    _connection_permit: OwnedSemaphorePermit,
    _global_permit: OwnedSemaphorePermit,
    created: Instant,
    published: AtomicBool,
    retired_notice: AtomicBool,
    reconciles: AtomicUsize,
    active: AtomicBool,
    progress: Mutex<Progress>,
    changed: Notify,
}
impl Operation {
    fn retire_write(&self) {
        self.write.retirement().end_scope();
        if !self.retired_notice.swap(true, Ordering::AcqRel) {
            if let Some(c) = self.connection.upgrade() {
                c.selection.notice(&self.id);
            }
        }
        self.changed.notify_waiters();
    }
    fn write_current(&self) -> Result<()> {
        if Instant::now() >= self.created + LEASE_TTL {
            return Err(unavailable());
        }
        self.write.retirement().check_current().map_err(denied)
    }
    fn disclosure_current(&self) -> Result<()> {
        self.disclosure
            .retirement()
            .check_current()
            .map_err(denied)?;
        let p = self.progress.lock().map_err(denied)?;
        if p.settled
            .is_some_and(|at| Instant::now() >= at + RECEIPT_TTL)
            || (p.settled.is_none()
                && !p.worker_started
                && Instant::now() >= self.created + LEASE_TTL)
        {
            return Err(unavailable());
        }
        Ok(())
    }
    fn complete(&self, receipt: Receipt) {
        {
            let mut p = self
                .progress
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if p.receipt.is_some() {
                return;
            }
            p.receipt = Some(receipt);
            p.settled = Some(Instant::now());
        }
        // The original outcome is retained before retirement, notification or disclosure.
        self.retire_write();
        self.changed.notify_waiters();
    }
    fn no_start(&self, code: Failure) {
        {
            let mut p = self
                .progress
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if p.worker_started || p.receipt.is_some() || p.command.is_none() {
                return;
            }
            p.receipt = Some(Receipt {
                result: Outcome::Failed { code },
                persistence: Persistence::NotAttempted,
            });
            p.settled = Some(Instant::now());
        }
        self.retire_write();
    }
    fn attempt(&self) -> Result<Attempt> {
        let p = self.progress.lock().map_err(denied)?;
        let attempt = if let Some(receipt) = &p.receipt {
            AttemptState::Settled {
                receipt: Box::new(receipt.clone()),
            }
        } else if p.command.is_some() {
            AttemptState::Pending
        } else {
            AttemptState::NotStarted
        };
        Ok(Attempt {
            selection_id: self.id.clone(),
            root: self.snapshot.root().clone(),
            attempt,
        })
    }
    fn observe_budget(&self) -> Result<()> {
        self.reconciles
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                if n < RECONCILE_LIMIT {
                    n.checked_add(1)
                } else {
                    None
                }
            })
            .map_err(denied)?;
        Ok(())
    }
}
fn root(query: &Query) -> RepositoryRootId {
    RepositoryRootId {
        workspace_id: query.workspace_id.clone(),
        kind: query
            .git_root_id
            .as_ref()
            .map_or(RepositoryRootKind::Primary, |id| {
                RepositoryRootKind::Registered {
                    git_root_id: id.clone(),
                }
            }),
    }
}
fn keys(query: &Query) -> Vec<RepositoryLifecycleKey> {
    let mut keys = vec![
        RepositoryLifecycleKey::Database,
        RepositoryLifecycleKey::WireAuthority,
        RepositoryLifecycleKey::Workspace(query.workspace_id.clone()),
    ];
    if let Some(id) = &query.git_root_id {
        keys.push(RepositoryLifecycleKey::GitRoot(id.clone()));
    }
    keys
}
fn snapshot(source: &StoreSnapshot) -> Result<Snapshot> {
    if source.binding().is_none() {
        return Err(unavailable());
    }
    let selection = match source.selection().ok_or_else(unavailable)? {
        RepositoryStoredSelection::NeverSaved => SelectionState::NeverSaved,
        RepositoryStoredSelection::Reset => SelectionState::Reset,
        RepositoryStoredSelection::Saved(value) => SelectionState::Saved {
            value: value.clone(),
        },
    };
    Ok(Snapshot {
        root: source.root().clone(),
        root_incarnation: source
            .root_incarnation()
            .ok_or_else(unavailable)?
            .get()
            .to_string(),
        selection_revision: source
            .selection_revision()
            .ok_or_else(unavailable)?
            .get()
            .to_string(),
        selection,
    })
}
fn projection(outcome: RepositorySelectionWriteOutcome) -> Receipt {
    let persistence = match outcome.persistence {
        StorePersistence::NotAttempted => Persistence::NotAttempted,
        StorePersistence::NoEffect => Persistence::NoEffect,
        StorePersistence::Committed { revision } => Persistence::Committed {
            selection_revision: revision.get().to_string(),
        },
        StorePersistence::Unknown => Persistence::Unknown,
    };
    let result = match outcome.result {
        Ok(RepositorySelectionWriteResult::Applied(s)) => {
            snapshot(&s).map(|snapshot| Outcome::Applied { snapshot })
        }
        Ok(RepositorySelectionWriteResult::Unchanged(s)) => {
            snapshot(&s).map(|snapshot| Outcome::Unchanged { snapshot })
        }
        Ok(RepositorySelectionWriteResult::Conflict(s)) => {
            snapshot(&s).map(|snapshot| Outcome::Conflict { snapshot })
        }
        Ok(RepositorySelectionWriteResult::MissingRoot(_)) => Ok(Outcome::MissingRoot),
        Err(Error::Forbidden(_)) => Ok(Outcome::Failed {
            code: Failure::AdmissionRetired,
        }),
        Err(_) => Ok(Outcome::Failed {
            code: Failure::StorageFailed,
        }),
    }
    .unwrap_or(Outcome::Failed {
        code: Failure::CompletionUnobserved,
    });
    Receipt {
        result,
        persistence,
    }
}

tokio::task_local! { static SELECTION_REQUEST: Arc<Request>; }
struct Request {
    connection: Arc<Connection>,
    weak: Weak<Self>,
    frame: Frame,
    lifetime: Option<RepositorySourceLifetime>,
    _subscriptions: Vec<RepositorySubscription>,
    target: Mutex<Option<Arc<Operation>>>,
    initiator: AtomicBool,
    owns_lane: bool,
    claimed: AtomicBool,
    completed: AtomicBool,
    consumed: AtomicBool,
    public: AtomicBool,
    entry_error: Option<bool>, // true: changed command; false: unavailable
    created: Instant,
}
impl Request {
    fn check(&self) -> Result<()> {
        self.connection.entered().map_err(denied)?;
        self.connection.selection.check()?;
        if self.completed.load(Ordering::Acquire) || Instant::now() >= self.created + FRAME_TTL {
            return Err(unavailable());
        }
        self.lifetime
            .as_ref()
            .ok_or_else(unavailable)?
            .retirement()
            .check_current()
            .map_err(denied)
    }
    fn current(services: &Services, frame: &Frame) -> Result<Arc<Self>> {
        let request = SELECTION_REQUEST.try_with(Clone::clone).map_err(denied)?;
        request.check()?;
        if !std::ptr::eq(request.connection.services.as_ref(), services)
            || &request.frame != frame
            || request.claimed.swap(true, Ordering::AcqRel)
        {
            return Err(unavailable());
        }
        match request.entry_error {
            Some(true) => Err(changed_command()),
            Some(false) => Err(unavailable()),
            None => Ok(request),
        }
    }
    fn operation(&self) -> Result<Arc<Operation>> {
        self.target
            .lock()
            .map_err(denied)?
            .clone()
            .ok_or_else(unavailable)
    }
    fn finish(&self) {
        if self.completed.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(life) = &self.lifetime {
            life.retirement().end_scope();
        }
        if let Ok(op) = self.operation() {
            if self.owns_lane {
                op.active.store(false, Ordering::Release);
            }
            if self.initiator.load(Ordering::Acquire) {
                op.retire_write();
                op.no_start(Failure::AdmissionRetired);
            }
            if matches!(self.frame, Frame::Capture(_)) && !op.published.load(Ordering::Acquire) {
                op.retire_write();
                op.disclosure.retirement().end_scope();
            }
        }
    }
}
impl Drop for Request {
    fn drop(&mut self) {
        self.finish();
    }
}

pub(super) fn capture_frame(c: &Connection, frame: Frame) -> Arc<dyn RepositoryReadRequestScope> {
    let captured = (|| {
        c.entered().map_err(denied)?;
        c.selection.check()?;
        if SELECTION_REQUEST.try_with(|_| ()).is_ok()
            || super::NATIVE_REQUEST.try_with(|_| ()).is_ok()
        {
            return Err(unavailable());
        }
        let (life, registration) = c.new_lifetime().map_err(denied)?;
        let subscriptions = vec![
            registration,
            life.subscribe(&c.services.store, c.caller.caller(), &keys(&frame.query()))
                .map_err(denied)?,
        ];
        Ok((life, subscriptions))
    })();
    let (life, subscriptions) = match captured {
        Ok((life, subs)) => (Some(life), subs),
        Err(_) => (None, Vec::new()),
    };
    let mut target = None;
    let mut initiator = false;
    let mut owns_lane = false;
    let mut entry_error = life.is_none().then_some(false);
    if life.is_some() {
        if let Some((id, command)) = frame_target(&frame) {
            let record = c
                .selection
                .feed
                .lock()
                .ok()
                .and_then(|f| f.records.get(id).cloned());
            if let Some(op) =
                record.filter(|o| o.query == frame.query() && o.published.load(Ordering::Acquire))
            {
                let claim = (|| {
                    op.disclosure_current()?;
                    op.active
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                        .map_err(denied)?;
                    owns_lane = true;
                    if let Some(command) = command {
                        if !command.valid() {
                            return Err(Error::InvalidParams(
                                "Invalid repository remote name".into(),
                            ));
                        }
                        let mut p = op.progress.lock().map_err(denied)?;
                        if let Some(old) = &p.command {
                            if old != &command {
                                return Err(changed_command());
                            }
                            op.observe_budget()?;
                        } else {
                            op.write_current()?;
                            p.command = Some(command);
                            initiator = true;
                        }
                    } else if matches!(frame, Frame::Reconcile(_)) {
                        op.observe_budget()?;
                    }
                    Ok(())
                })();
                if let Err(e) = claim {
                    entry_error = Some(matches!(e, Error::InvalidParams(_)));
                }
                target = Some(op);
            } else if !matches!(frame, Frame::Release(_)) {
                entry_error = Some(false);
            }
        }
    }
    let request = Arc::new_cyclic(|weak| Request {
        connection: c.weak.upgrade().expect("owned connection"),
        weak: weak.clone(),
        frame,
        lifetime: life,
        _subscriptions: subscriptions,
        target: Mutex::new(target),
        initiator: AtomicBool::new(initiator),
        owns_lane,
        claimed: AtomicBool::new(false),
        completed: AtomicBool::new(false),
        consumed: AtomicBool::new(false),
        public: AtomicBool::new(false),
        entry_error,
        created: Instant::now(),
    });
    if let Some(life) = &request.lifetime {
        let retirement = life.retirement();
        let weak = Arc::downgrade(&request);
        tokio::spawn(async move {
            tokio::select! { () = retirement.native_cancelled() => {}, () = tokio::time::sleep(FRAME_TTL) => {} }
            if let Some(request) = weak.upgrade() {
                request.finish();
            }
        });
    }
    request
}
fn frame_target(frame: &Frame) -> Option<(&str, Option<Command>)> {
    match frame {
        Frame::Capture(_) => None,
        Frame::Save(q) => Some((&q.selection_id, Some(Command::Save(q.choice.clone())))),
        Frame::Reset(q) => Some((&q.selection_id, Some(Command::Reset))),
        Frame::Reconcile(q) | Frame::Release(q) => Some((&q.selection_id, None)),
    }
}

async fn authority(request: &Request) -> Result<RepositoryAuthoritySnapshot> {
    request.check()?;
    let c = &request.connection;
    let query = request.frame.query();
    let Caller::Wire {
        principal_id,
        host_role,
    } = c.caller.caller()
    else {
        return Err(unavailable());
    };
    let hash = token_hash(c);
    let before = c
        .services
        .store
        .repository_authority_snapshot(&query.workspace_id, principal_id, hash)
        .await
        .map_err(denied)?;
    if before.workspace.value.is_none() || before.principal.value.is_none() {
        return Err(Error::NotFound("Workspace not found".into()));
    }
    c.services
        .require_workspace_manager(&query.workspace_id, "Repository selection")
        .await
        .map_err(|error| match error {
            Error::NotFound(_) => Error::NotFound("Workspace not found".into()),
            _ => unavailable(),
        })?;
    if &c
        .services
        .store
        .get_host_role(principal_id)
        .await
        .map_err(denied)?
        != host_role
    {
        return Err(unavailable());
    }
    let credential = if let Some(hash) = hash {
        c.services
            .store
            .lookup_principal_credential(hash)
            .await
            .map_err(denied)?
    } else {
        None
    };
    let after = c
        .services
        .store
        .repository_authority_snapshot(&query.workspace_id, principal_id, hash)
        .await
        .map_err(denied)?;
    if before != after {
        return Err(unavailable());
    }
    let facts = RepositoryAuthorityFacts {
        caller: c.caller.caller().clone(),
        workspace: query.workspace_id.clone(),
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
    c.caller
        .verify(&facts, &query.workspace_id)
        .map_err(denied)?;
    request.check()?;
    Ok(before)
}
fn token_hash(c: &Connection) -> Option<&str> {
    match c.caller.wire_credential() {
        Some(WireCredential::Principal { token_hash, .. }) => Some(token_hash),
        _ => None,
    }
}
async fn disclosure(request: &Request, op: &Operation) -> Result<()> {
    request.check()?;
    op.disclosure_current()?;
    if authority(request).await? != op.authority {
        return Err(unavailable());
    }
    let current = request
        .connection
        .services
        .store
        .repository_selection_snapshot(op.snapshot.root())
        .await
        .map_err(denied)?;
    if current.binding().is_none()
        || current.root_incarnation() != op.snapshot.root_incarnation()
        || current.binding() != op.snapshot.binding()
    {
        return Err(unavailable());
    }
    request.check()?;
    op.disclosure_current()
}

impl RepositoryReadRequestScope for Request {
    fn scope<'a>(&'a self, body: BoxFuture<'a, ()>) -> BoxFuture<'a, ()> {
        Box::pin(SELECTION_REQUEST.scope(self.weak.upgrade().expect("owned selection frame"), body))
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
            if self.public.load(Ordering::Acquire) || self.entry_error.is_some() {
                if self.consumed.swap(true, Ordering::AcqRel) {
                    return Err(unavailable());
                }
                return transfer();
            }
            entered?;
            let request = self.weak.upgrade().ok_or_else(unavailable)?;
            let op = self.operation()?;
            let operation = async {
                let _legacy = self
                    .connection
                    .caller
                    .legacy_lease()
                    .await
                    .map_err(denied)?;
                disclosure(self, &op).await?;
                self.check()?;
                self.connection
                    .parent
                    .native_dispatch(|| {
                        self.lifetime
                            .as_ref()
                            .ok_or(AdmissionError::Unavailable)?
                            .retirement()
                            .native_dispatch(|| {
                                op.disclosure.retirement().native_dispatch(|| {
                                    if self.completed.load(Ordering::Acquire)
                                        || Instant::now() >= self.created + FRAME_TTL
                                        || self.consumed.swap(true, Ordering::AcqRel)
                                    {
                                        return Err(AdmissionError::Retired);
                                    }
                                    let result =
                                        transfer().map_err(|_| AdmissionError::Unavailable);
                                    if result.is_ok() && matches!(self.frame, Frame::Capture(_)) {
                                        op.published.store(true, Ordering::Release);
                                    }
                                    result
                                })
                            })
                    })
                    .map_err(denied)
            };
            checked(&request, async {
                tokio::time::timeout(ADMISSION_TIMEOUT, operation)
                    .await
                    .map_err(denied)?
            })
            .await
        })
    }
}

/// Check actual caller/credential on EVERY poll, including moved pending futures.
async fn checked<T>(
    request: &Arc<Request>,
    body: impl Future<Output = Result<T>> + Send,
) -> Result<T> {
    request.check()?;
    let retirement = request
        .lifetime
        .as_ref()
        .ok_or_else(unavailable)?
        .retirement();
    let future = async {
        tokio::select! {
            value = body => value,
            () = retirement.native_cancelled() => Err(unavailable()),
        }
    };
    tokio::pin!(future);
    std::future::poll_fn(|cx| {
        if let Err(error) = request.check() {
            return std::task::Poll::Ready(Err(error));
        }
        future.as_mut().poll(cx)
    })
    .await
}
fn entry<'a, T: Send + 'static>(
    services: &'a Services,
    frame: &Frame,
    body: impl FnOnce(Arc<Request>) -> BoxFuture<'static, Result<T>> + Send + 'static,
) -> BoxFuture<'a, Result<T>> {
    let request = Request::current(services, frame);
    Box::pin(async move {
        let request = request?;
        let result = checked(&request, body(request.clone())).await;
        if result.is_err() {
            request.public.store(true, Ordering::Release);
            if request.initiator.load(Ordering::Acquire) {
                if let Ok(op) = request.operation() {
                    op.retire_write();
                    op.no_start(Failure::AuthorityUnavailable);
                }
            }
        }
        result
    })
}

pub(crate) fn capture(services: &Services, query: Query) -> BoxFuture<'_, Result<Capture>> {
    entry(services, &Frame::Capture(query), |request| {
        Box::pin(async move {
            tokio::time::timeout(ACQUIRE_TIMEOUT, acquire(&request))
                .await
                .map_err(denied)?
        })
    })
}
async fn acquire(request: &Arc<Request>) -> Result<Capture> {
    let c = &request.connection;
    let permit = c
        .selection
        .permits
        .clone()
        .try_acquire_owned()
        .map_err(denied)?;
    let global = c
        .services
        .repository_selection_capacity
        .records
        .clone()
        .try_acquire_owned()
        .map_err(denied)?;
    let query = request.frame.query();
    let (write, write_registration) = c.new_lifetime().map_err(denied)?;
    let (disclosure, disclosure_registration) = c.new_lifetime().map_err(denied)?;
    let subscriptions = vec![
        write_registration,
        disclosure_registration,
        write
            .subscribe(&c.services.store, c.caller.caller(), &keys(&query))
            .map_err(denied)?,
        disclosure
            .subscribe(&c.services.store, c.caller.caller(), &keys(&query))
            .map_err(denied)?,
    ];
    let _legacy = c.caller.legacy_lease().await.map_err(denied)?;
    let observed_authority = authority(request).await?;
    let original = c
        .services
        .store
        .repository_selection_snapshot(&root(&query))
        .await
        .map_err(denied)?;
    let public = snapshot(&original)?;
    if authority(request).await? != observed_authority {
        return Err(unavailable());
    }
    write.retirement().check_current().map_err(denied)?;
    disclosure.retirement().check_current().map_err(denied)?;
    let id = uuid::Uuid::new_v4().to_string();
    let scope = ExecutionScope {
        daemon_id: c.services.daemon_boot_id.clone(),
        authority_scope_id: id.clone(),
        authority_generation: observed_authority
            .workspace
            .revision
            .ok_or_else(unavailable)?
            .get(),
    };
    let op = Arc::new(Operation {
        id: id.clone(),
        connection: c.weak.clone(),
        query,
        snapshot: original,
        authority: observed_authority,
        write,
        disclosure,
        _subscriptions: subscriptions,
        _connection_permit: permit,
        _global_permit: global,
        created: Instant::now(),
        published: AtomicBool::new(false),
        retired_notice: AtomicBool::new(false),
        reconciles: AtomicUsize::new(0),
        active: AtomicBool::new(false),
        progress: Mutex::default(),
        changed: Notify::new(),
    });
    *request.target.lock().map_err(denied)? = Some(op.clone());
    let sequence = {
        let mut feed = c.selection.feed.lock().map_err(denied)?;
        if feed.closed {
            return Err(unavailable());
        }
        feed.records.insert(id.clone(), op.clone());
        feed.sequence
    };
    maintain(op);
    Ok(Capture {
        selection_id: id,
        scope,
        root: public.root.clone(),
        snapshot: public,
        retirement_sequence: sequence.to_string(),
        expires_after_ms: 300_000,
    })
}
fn maintain(op: Arc<Operation>) {
    tokio::spawn(async move {
        let mut write_retired = false;
        loop {
            let wake = op.changed.notified();
            tokio::pin!(wake);
            wake.as_mut().enable();
            let (deadline, pending) = {
                let p = op
                    .progress
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                (
                    p.settled
                        .map_or(op.created + LEASE_TTL, |at| at + RECEIPT_TTL),
                    p.worker_started && p.receipt.is_none(),
                )
            };
            let undisclosable = op.disclosure.retirement().check_current().is_err();
            if undisclosable && !pending {
                if let Some(c) = op.connection.upgrade() {
                    c.selection
                        .feed
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .records
                        .remove(&op.id);
                }
                return;
            }
            if Instant::now() >= deadline {
                if !write_retired {
                    write_retired = true;
                    op.retire_write();
                }
                if !pending {
                    op.disclosure.retirement().end_scope();
                    if let Some(c) = op.connection.upgrade() {
                        c.selection
                            .feed
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .records
                            .remove(&op.id);
                    }
                    return;
                }
                wake.await;
                continue;
            }
            let retirement = op.write.retirement();
            tokio::select! {
                () = wake => {},
                () = retirement.native_cancelled(), if !write_retired => { write_retired = true; op.retire_write(); },
                () = tokio::time::sleep_until(deadline) => {},
            }
        }
    });
}
pub(crate) fn save(services: &Services, query: SaveQuery) -> BoxFuture<'_, Result<Attempt>> {
    entry(services, &Frame::Save(query), |request| {
        Box::pin(async move { mutate(&request).await })
    })
}
pub(crate) fn reset(services: &Services, query: BoundQuery) -> BoxFuture<'_, Result<Attempt>> {
    entry(services, &Frame::Reset(query), |request| {
        Box::pin(async move { mutate(&request).await })
    })
}
pub(crate) fn reconcile(services: &Services, query: BoundQuery) -> BoxFuture<'_, Result<Attempt>> {
    entry(services, &Frame::Reconcile(query), |request| {
        Box::pin(async move {
            let op = request.operation()?;
            disclosure(&request, &op).await?;
            op.attempt()
        })
    })
}
pub(crate) fn release(services: &Services, query: BoundQuery) -> BoxFuture<'_, Result<Released>> {
    entry(services, &Frame::Release(query), |request| {
        Box::pin(async move {
            if let Ok(op) = request.operation() {
                op.retire_write();
                op.no_start(Failure::AdmissionRetired);
            }
            request.public.store(true, Ordering::Release);
            Ok(Released { released: true })
        })
    })
}
async fn mutate(request: &Arc<Request>) -> Result<Attempt> {
    let op = request.operation()?;
    disclosure(request, &op).await?;
    if request.initiator.load(Ordering::Acquire) {
        op.write_current()?;
        let permit = request
            .connection
            .services
            .repository_selection_capacity
            .workers
            .clone()
            .try_acquire_owned()
            .map_err(denied)?;
        let legacy = request
            .connection
            .caller
            .legacy_lease()
            .await
            .map_err(denied)?;
        request.check()?;
        op.write_current()?;
        {
            let mut p = op.progress.lock().map_err(denied)?;
            if p.worker_started || p.receipt.is_some() {
                drop(p);
                return op.attempt();
            }
            p.worker_started = true;
        }
        let owned = request.clone();
        let operation = op.clone();
        tokio::spawn(async move {
            let c = owned.connection.clone();
            with_caller(
                c.caller.caller().clone(),
                with_wire_credential(c.caller.wire_credential().cloned(), async move {
                    let _legacy = legacy;
                    let outcome = observe_store(run_write(&owned, &operation)).await;
                    let receipt = outcome.unwrap_or(Receipt {
                        result: Outcome::Failed {
                            code: Failure::CompletionUnobserved,
                        },
                        persistence: Persistence::Unknown,
                    });
                    operation.complete(receipt);
                    // Capacity represents true worker completion, independent of the waiter.
                    drop(permit);
                }),
            )
            .await;
        });
    }
    let wait = async {
        loop {
            let wake = op.changed.notified();
            tokio::pin!(wake);
            wake.as_mut().enable();
            let attempt = op.attempt()?;
            if !matches!(attempt.attempt, AttemptState::Pending) {
                return Ok(attempt);
            }
            wake.await;
        }
    };
    match tokio::time::timeout(ADMISSION_TIMEOUT, wait).await {
        Ok(result) => result,
        Err(_) => op.attempt(),
    }
}
async fn observe_store(future: impl Future<Output = Receipt>) -> std::thread::Result<Receipt> {
    tokio::pin!(future);
    std::future::poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(std::task::Poll::Ready(value)) => std::task::Poll::Ready(Ok(value)),
            Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
            Err(error) => std::task::Poll::Ready(Err(error)),
        }
    })
    .await
}
async fn run_write(request: &Request, op: &Operation) -> Receipt {
    let command = match op.progress.lock() {
        Ok(p) => p.command.clone(),
        Err(_) => None,
    };
    let Some(command) = command else {
        return Receipt {
            result: Outcome::Failed {
                code: Failure::AdmissionRetired,
            },
            persistence: Persistence::NotAttempted,
        };
    };
    let c = &request.connection;
    let result = c
        .services
        .store
        .write_repository_selection_admitted(
            &op.snapshot,
            command.change(),
            &op.authority,
            token_hash(c),
            || {
                request.check()?;
                op.write_current()?;
                if Instant::now() >= request.created + ADMISSION_TIMEOUT {
                    return Err(unavailable());
                }
                c.parent
                    .native_dispatch(|| {
                        request
                            .lifetime
                            .as_ref()
                            .ok_or(AdmissionError::Unavailable)?
                            .retirement()
                            .native_dispatch(|| {
                                op.write.retirement().native_dispatch(|| {
                                    if request.completed.load(Ordering::Acquire)
                                        || Instant::now() >= request.created + ADMISSION_TIMEOUT
                                    {
                                        return Err(AdmissionError::Retired);
                                    }
                                    let mut p =
                                        op.progress.lock().map_err(|_| AdmissionError::Retired)?;
                                    if p.admitted || p.receipt.is_some() {
                                        return Err(AdmissionError::Retired);
                                    }
                                    p.admitted = true;
                                    Ok(())
                                })
                            })
                    })
                    .map_err(denied)
            },
        )
        .await;
    #[cfg(test)]
    {
        let gate = c.selection.after_store.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.wait().await;
        }
    }
    projection(result)
}

#[cfg(test)]
#[path = "native_selection/tests.rs"]
mod tests;
