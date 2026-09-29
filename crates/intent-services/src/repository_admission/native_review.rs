//! Original native socket review ownership. No public ID or read/selection lease
//! supplies a write grant. Completed effects outlive admission, not disclosure.
use super::{Connection, Services};
use crate::repository_admission::lifecycle::{RepositorySourceLifetime, RepositorySubscription};
use crate::repository_admission::{
    self as engine, AdmissionError, AdmissionResult, OriginalRepositoryCaller,
    RepositoryCompletion, RepositoryEntry, RepositoryOperationAdmission, RepositoryOperationFacts,
    RepositoryRetirement,
};
use crate::repository_admission_git_source::{RepositoryGitSource, RootRecord};
use crate::repository_admission_sources::{
    with_repository_lifecycle_source_observed, RepositorySourceInput,
};
use crate::repository_context_live::{resolver, target_context, SelectionFacts};
use crate::repository_context_reader::{
    read_context_root_with_resolver, AdmittedRepositoryRoot, GitConfigEnvironment,
    RepositoryContextInput, RepositoryObservedRoot,
};
use crate::repository_credentials::authority::{
    CredentialFuture, RepositoryAuthorityFence, RepositoryCredentialTransport,
};
use crate::repository_credentials::{
    BoundGitlabRequestCredentials, RepositoryAuthority, RepositoryAuthorityRequest,
    RepositoryCredentialError as CredentialError, RepositoryCredentialUse as Use,
};
use crate::settings_registry::SettingsSnapshot;
use crate::source_control_auth_ops::repository_owner::RepositoryConnectionFacts;
use intent_core::caller::{with_caller, with_wire_credential, WireCredential};
use intent_core::repository_request::{
    NativeReviewBoundQuery as Bound, NativeReviewChoice as Choice,
    NativeReviewExecuteQuery as Execute, NativeReviewFrame as Frame, NativeReviewOperationCapture,
    NativeReviewPrepareQuery as Prepare, NativeReviewRetired as Retired, NativeReviewRetirements,
    RepositoryReadReplyKind, RepositoryReadRequestScope,
};
use intent_core::{
    BoxFuture, Caller, Error, NativeReviewExecution as Execution,
    NativeReviewGitReceipt as GitReceipt, NativeReviewOutcome as Outcome,
    NativeReviewPreparation as Preparation, NativeReviewPublication as Publication,
    NativeReviewStage as Stage, RepositoryProvider, RepositoryRootId, RepositoryRootKind,
    RepositoryTarget, Result,
};
use intent_sourcecontrol::{
    GitLabSourceControl, NewPullRequest, ReviewBranchIdentity, ReviewCreateOutcome, SourceControl,
};
use intent_store::{
    RepositoryAuthoritySnapshot, RepositoryLifecycleKey, RepositorySelectionSnapshot,
    RepositoryStoredSelection,
};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

const RECORDS: usize = 32;
const GLOBAL_RECORDS: usize = 256;
const WORKERS: usize = 2;
const GLOBAL_WORKERS: usize = 8;
const NOTICES: usize = 64;
const OBSERVATIONS: usize = 64;
const COMMAND_BYTES: usize = 65_536;
const ACQUIRE: Duration = Duration::from_secs(5);
const FRAME_TTL: Duration = Duration::from_secs(15);
const LEASE_TTL: Duration = Duration::from_secs(300);
const RECEIPT_TTL: Duration = Duration::from_secs(600);
const STAGE_TTL: Duration = Duration::from_secs(120);
fn unavailable() -> Error {
    Error::Forbidden("Repository review unavailable".into())
}
fn denied(_: impl std::fmt::Debug) -> Error {
    unavailable()
}
fn invalid() -> Error {
    Error::InvalidParams("Unsupported or changed native review parameters".into())
}
fn local(_: impl std::fmt::Debug) -> AdmissionError {
    AdmissionError::Unavailable
}
fn credential_error(_: impl std::fmt::Debug) -> CredentialError {
    CredentialError::AuthorityDenied
}

pub(crate) struct Capacity {
    records: Arc<Semaphore>,
    workers: Arc<Semaphore>,
    creates: Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>,
}
impl Default for Capacity {
    fn default() -> Self {
        Self {
            records: Arc::new(Semaphore::new(GLOBAL_RECORDS)),
            workers: Arc::new(Semaphore::new(GLOBAL_WORKERS)),
            creates: Mutex::default(),
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
    feed: Mutex<Feed>,
    notify: Notify,
    records: Arc<Semaphore>,
    workers: Arc<Semaphore>,
}
impl Default for ConnectionState {
    fn default() -> Self {
        Self {
            feed: Mutex::default(),
            notify: Notify::new(),
            records: Arc::new(Semaphore::new(RECORDS)),
            workers: Arc::new(Semaphore::new(WORKERS)),
        }
    }
}
impl ConnectionState {
    fn check(&self) -> Result<()> {
        let f = self.feed.lock().map_err(denied)?;
        if f.taken && !f.closed {
            Ok(())
        } else {
            Err(unavailable())
        }
    }
    pub(super) fn close(&self) {
        let ops = {
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
                operation_ids: vec![],
                sequence: f.sequence.to_string(),
                all_retired: true,
                terminal: true,
            });
            f.records.values().cloned().collect::<Vec<_>>()
        };
        for op in ops {
            op.retire();
            op.disclosure.retirement().end_scope();
        }
        self.notify.notify_waiters();
    }
    fn notice(&self, id: &str) {
        let overflow = {
            let mut f = self
                .feed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if f.closed {
                return;
            }
            if let Some(n) = f
                .sequence
                .checked_add(1)
                .filter(|_| f.notices.len() < NOTICES)
            {
                f.sequence = n;
                f.notices.push_back(Retired {
                    operation_ids: vec![id.into()],
                    sequence: n.to_string(),
                    all_retired: false,
                    terminal: false,
                });
                false
            } else {
                true
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
            c.review.close();
        }
    }
}
impl NativeReviewRetirements for Receiver {
    fn next(&mut self) -> BoxFuture<'_, Option<Retired>> {
        Box::pin(async move {
            let c = self.connection.upgrade()?;
            loop {
                let wake = c.review.notify.notified();
                tokio::pin!(wake);
                wake.as_mut().enable();
                {
                    let mut f = c
                        .review
                        .feed
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if let Some(t) = f.terminal.take() {
                        return Some(t);
                    }
                    if f.closed {
                        return None;
                    }
                    if let Some(n) = f.notices.pop_front() {
                        return Some(n);
                    }
                }
                wake.await;
            }
        })
    }
}
pub(super) fn take_retirements(c: &Connection) -> Option<Box<dyn NativeReviewRetirements>> {
    let mut f = c.review.feed.lock().ok()?;
    if f.taken || f.closed {
        return None;
    }
    f.taken = true;
    Some(Box::new(Receiver {
        connection: c.weak.clone(),
    }))
}

struct CapturedMetadata {
    connection: Weak<Connection>,
    root: RootRecord,
    selection: RepositorySelectionSnapshot,
    authority: RepositoryAuthoritySnapshot,
    provider: Arc<RepositoryConnectionFacts>,
    settings: Option<(Arc<crate::SettingsRegistry>, Arc<SettingsSnapshot>)>,
}
impl CapturedMetadata {
    async fn validate(&self, execution: bool) -> Result<Arc<Connection>> {
        let c = self.connection.upgrade().ok_or_else(unavailable)?;
        c.entered().map_err(denied)?;
        c.review.check()?;
        if authority(&c, self.root.root()).await? != self.authority {
            return Err(unavailable());
        }
        let selection = c
            .services
            .store
            .repository_selection_snapshot(self.root.root())
            .await
            .map_err(denied)?;
        if selection.binding() != self.selection.binding()
            || selection.root_incarnation() != self.selection.root_incarnation()
            || (execution
                && (selection.selection_revision() != self.selection.selection_revision()
                    || selection.selection() != self.selection.selection()))
            || RootRecord::read(&c.services.store, self.root.root())
                .await
                .map_err(denied)?
                != self.root
        {
            return Err(unavailable());
        }
        if execution {
            self.with_metadata(|| Ok(())).map_err(denied)?;
        }
        Ok(c)
    }
    fn with_settings<T>(&self, action: impl FnOnce() -> AdmissionResult<T>) -> AdmissionResult<T> {
        match &self.settings {
            Some((registry, snapshot)) => registry.with_original_snapshot(snapshot, |current| {
                if current {
                    action()
                } else {
                    Err(AdmissionError::Retired)
                }
            }),
            None => action(),
        }
    }
    fn with_metadata<T: Send>(
        &self,
        action: impl FnOnce() -> AdmissionResult<T> + Send,
    ) -> AdmissionResult<T> {
        self.with_settings(|| {
            let mut result = None;
            RepositoryConnectionFacts::with_native_review_current(&self.provider, |current| {
                if current {
                    result = Some(action());
                }
                Ok(())
            })
            .map_err(local)?;
            result.ok_or(AdmissionError::Retired)?
        })
    }
}
#[derive(Default)]
struct Progress {
    command: Option<Execute>,
    started: bool,
    settled: Option<Instant>,
    engine: Option<RepositoryOperationAdmission>,
    execution: Option<Execution>,
    effects: Vec<GitReceipt>,
    review_effect: Option<Outcome>,
}
// This private intent is independent of historical disclosure and ordinary write
// retirement. Its original lifecycle still cancels pending/published children.
struct Companion {
    lifetime: RepositorySourceLifetime,
    state: Mutex<CompanionState>,
}
#[derive(Default)]
struct CompanionState {
    witness: Option<CommitWitness>,
    normal: bool,
    delivered: bool,
    closed: bool,
    capture: Option<String>,
}
#[derive(Clone)]
struct CommitWitness {
    observed: RepositoryObservedRoot,
    staging: String,
}
struct Operation {
    id: String,
    query: Prepare,
    metadata: Arc<CapturedMetadata>,
    facts: RepositoryOperationFacts,
    observed: RepositoryObservedRoot,
    files: Vec<String>,
    content_fingerprint: String,
    write: RepositorySourceLifetime,
    disclosure: RepositorySourceLifetime,
    _subscriptions: Vec<RepositorySubscription>,
    _record: OwnedSemaphorePermit,
    _global: OwnedSemaphorePermit,
    created: Instant,
    published: AtomicBool,
    noticed: AtomicBool,
    observations: AtomicUsize,
    progress: Mutex<Progress>,
    changed: Notify,
    companion: Option<Companion>,
    companion_publication: Option<std::sync::OnceLock<Instant>>,
}
impl Operation {
    fn retire(&self) {
        if let Some(companion) = &self.companion {
            // Retirement precedes the state lock: capture/delivery always enter
            // this lifetime before taking that lock, never in the reverse order.
            companion.lifetime.retirement().end_scope();
            companion
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .closed = true;
        }
        self.retire_write();
    }
    fn retire_write(&self) {
        self.write.retirement().end_scope();
        if !self.noticed.swap(true, Ordering::AcqRel) {
            if let Some(c) = self.metadata.connection.upgrade() {
                c.review.notice(&self.id);
            }
        }
        self.changed.notify_waiters();
    }
    fn lease_start(&self) -> Instant {
        self.companion_publication
            .as_ref()
            .and_then(|p| p.get().copied())
            .unwrap_or(self.created)
    }
    fn write_current(&self) -> Result<()> {
        if Instant::now() >= self.lease_start() + LEASE_TTL {
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
            || (!p.started
                && p.settled.is_none()
                && Instant::now() >= self.lease_start() + LEASE_TTL)
        {
            return Err(unavailable());
        }
        Ok(())
    }
    fn budget(&self) -> Result<()> {
        self.observations
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < OBSERVATIONS).then(|| n + 1)
            })
            .map_err(denied)?;
        Ok(())
    }
    fn primitive(&self, effect: GitReceipt) {
        let mut p = self
            .progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !p.effects.contains(&effect) {
            p.effects.push(effect);
        }
        self.changed.notify_waiters();
    }
    fn primitive_review(&self, outcome: Outcome) {
        let mut progress = self
            .progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if progress.review_effect.is_none() {
            progress.review_effect = Some(outcome);
        }
        self.changed.notify_waiters();
    }
    fn retain(&self, execution: Execution) {
        let mut p = self
            .progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        p.execution = Some(execution);
        self.changed.notify_waiters();
    }
    fn complete(&self) {
        self.complete_owned(false);
    }
    fn complete_owned(&self, normal: bool) {
        let mut companion_normal = false;
        if normal && self.write_current().is_ok() {
            if let Some(companion) = &self.companion {
                let _ = companion.lifetime.retirement().native_dispatch(|| {
                    let mut state = companion.state.lock().map_err(local)?;
                    let p = self.progress.lock().map_err(local)?;
                    let successful = p
                        .engine
                        .as_ref()
                        .and_then(|e| e.execution().ok())
                        .is_some_and(|e| {
                            matches!(e.outcome, Outcome::NotAttempted)
                                && e.git_receipts.len() == 1
                                && matches!(e.git_receipts[0], GitReceipt::Commit { .. })
                        });
                    if !state.closed && state.witness.is_some() && successful {
                        state.normal = true;
                        companion_normal = true;
                    }
                    Ok(())
                });
            }
        }
        {
            let mut p = self
                .progress
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if p.settled.is_none() {
                p.execution = p
                    .engine
                    .as_ref()
                    .and_then(|e| e.execution().ok())
                    .or_else(|| p.execution.clone())
                    .or_else(|| {
                        Some(self.empty_execution(Outcome::Failed {
                            stage: self.query.action,
                            code: Some("repository-admission-retired".into()),
                            message: "Repository operation could not start".into(),
                        }))
                    });
                p.settled = Some(Instant::now());
            }
        }
        if companion_normal {
            self.retire_write();
        } else {
            self.retire();
        }
        self.changed.notify_waiters();
    }
    fn companion_normal(&self) -> bool {
        self.companion
            .as_ref()
            .is_some_and(|c| c.state.lock().is_ok_and(|s| s.normal && !s.closed))
    }
    fn claim_companion(&self, capture: &str) -> Result<()> {
        let companion = self.companion.as_ref().ok_or_else(unavailable)?;
        companion
            .lifetime
            .retirement()
            .native_dispatch(|| {
                let mut state = companion.state.lock().map_err(local)?;
                if Instant::now() >= self.created + LEASE_TTL
                    || state.closed
                    || !state.normal
                    || !state.delivered
                    || state.witness.is_none()
                    || state.capture.is_some()
                {
                    return Err(AdmissionError::Retired);
                }
                state.capture = Some(capture.to_owned());
                Ok(())
            })
            .map_err(denied)
    }
    fn companion_witness(&self, capture: &str) -> Result<CommitWitness> {
        let companion = self.companion.as_ref().ok_or_else(unavailable)?;
        companion
            .lifetime
            .retirement()
            .native_dispatch(|| {
                let state = companion.state.lock().map_err(local)?;
                if Instant::now() >= self.created + LEASE_TTL
                    || state.closed
                    || !state.normal
                    || !state.delivered
                    || state.capture.as_deref() != Some(capture)
                {
                    return Err(AdmissionError::Retired);
                }
                state.witness.clone().ok_or(AdmissionError::Retired)
            })
            .map_err(denied)
    }
    fn empty_execution(&self, outcome: Outcome) -> Execution {
        Execution {
            request_id: self.id.clone(),
            preparation: self.facts.preparation.clone(),
            git_receipts: vec![],
            outcome,
            publication: Publication::Unknown {
                local_head_sha: self.facts.preparation.local_head_sha.clone(),
                remote_source_sha: None,
            },
        }
    }
    fn state(&self, admin: bool) -> Result<Value> {
        let p = self.progress.lock().map_err(denied)?;
        let mut value = json!({"operationId":self.id,"root":self.query.review.root,"state":if p.settled.is_some(){"settled"}else if p.command.is_some(){"pending"}else{"prepared"}});
        if p.settled.is_some() {
            let mut e = p.execution.clone().ok_or_else(unavailable)?;
            for effect in &p.effects {
                if !e.git_receipts.contains(effect) {
                    e.git_receipts.push(effect.clone());
                }
            }
            if let Some(outcome) = &p.review_effect {
                e.outcome = outcome.clone();
            }
            let preparation = project(&e.preparation, admin);
            value["reviewExecution"] = json!(e);
            value["reviewExecution"]["preparation"] = preparation;
        }
        Ok(value)
    }
}
fn project(preparation: &Preparation, admin: bool) -> Value {
    let mut value = json!(preparation);
    if !admin {
        for target in ["source", "target"] {
            if let Some(target) = value[target].as_object_mut() {
                target.remove("connection");
            }
        }
    }
    value
}

fn keys(root: &RepositoryRootId) -> Vec<RepositoryLifecycleKey> {
    let mut v = vec![
        RepositoryLifecycleKey::Database,
        RepositoryLifecycleKey::WireAuthority,
        RepositoryLifecycleKey::Workspace(root.workspace_id.clone()),
        SelectionFacts::key(root),
    ];
    if let RepositoryRootKind::Registered { git_root_id } = &root.kind {
        v.push(RepositoryLifecycleKey::GitRoot(git_root_id.clone()));
    }
    v
}

tokio::task_local! {static REVIEW_REQUEST:Arc<Request>;}
struct Request {
    connection: Arc<Connection>,
    weak: Weak<Self>,
    frame: Frame,
    lifetime: Option<RepositorySourceLifetime>,
    subscriptions: Mutex<Vec<RepositorySubscription>>,
    provider: Option<Arc<RepositoryConnectionFacts>>,
    target: Mutex<Option<Arc<Operation>>>,
    initiator: bool,
    claimed: AtomicBool,
    completed: AtomicBool,
    consumed: AtomicBool,
    public: AtomicBool,
    entry_error: Option<bool>,
    created: Instant,
    admin_projection: AtomicBool,
    predecessor: Option<Arc<Operation>>,
    companion_delivered: AtomicBool,
    companion_reply: bool,
}
impl Request {
    fn check(&self) -> Result<()> {
        self.connection.entered().map_err(denied)?;
        self.connection.review.check()?;
        if self.completed.load(Ordering::Acquire)
            || (Instant::now() >= self.created + FRAME_TTL && !self.started())
        {
            return Err(unavailable());
        }
        self.lifetime
            .as_ref()
            .ok_or_else(unavailable)?
            .retirement()
            .check_current()
            .map_err(denied)
    }
    fn started(&self) -> bool {
        matches!(self.frame, Frame::Execute(_))
            && self
                .operation()
                .is_ok_and(|op| op.progress.lock().is_ok_and(|p| p.started))
    }
    fn current(s: &Services, f: &Frame) -> Result<Arc<Self>> {
        let r = REVIEW_REQUEST.try_with(Clone::clone).map_err(denied)?;
        r.check()?;
        if !std::ptr::eq(r.connection.services.as_ref(), s)
            || &r.frame != f
            || r.claimed.swap(true, Ordering::AcqRel)
        {
            return Err(unavailable());
        }
        match r.entry_error {
            Some(true) => Err(invalid()),
            Some(false) => Err(unavailable()),
            None => Ok(r),
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
        if let Some(l) = &self.lifetime {
            l.retirement().end_scope();
        }
        if let Ok(o) = self.operation() {
            if self.initiator {
                if self.companion_delivered.load(Ordering::Acquire) {
                    o.retire_write();
                } else {
                    o.retire();
                }
                let start = o
                    .progress
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .started;
                if !start {
                    o.complete();
                }
            }
            if matches!(self.frame, Frame::Prepare(_)) && !o.published.load(Ordering::Acquire) {
                o.retire();
                o.disclosure.retirement().end_scope();
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
    let capture = (|| {
        c.entered().map_err(denied)?;
        c.review.check()?;
        if REVIEW_REQUEST.try_with(|_| ()).is_ok()
            || super::NATIVE_REQUEST.try_with(|_| ()).is_ok()
            || frame.root().workspace_id != *frame.workspace()
        {
            return Err(unavailable());
        }
        if match &frame {
            Frame::Prepare(q) => serde_json::to_vec(q),
            Frame::Execute(q) => serde_json::to_vec(q),
            Frame::Reconcile(q) | Frame::Release(q) => serde_json::to_vec(q),
        }
        .map_err(denied)?
        .len()
            > COMMAND_BYTES
        {
            return Err(invalid());
        }
        let (life, registration) = c.new_lifetime().map_err(denied)?;
        let subs = vec![
            registration,
            life.subscribe(&c.services.store, c.caller.caller(), &keys(frame.root()))
                .map_err(denied)?,
        ];
        Ok((life, subs))
    })();
    let (lifetime, subscriptions) = match capture {
        Ok((l, s)) => (Some(l), s),
        Err(_) => (None, vec![]),
    };
    let mut target = None;
    let mut initiator = false;
    let mut predecessor = None;
    let mut error = lifetime.is_none().then_some(false);
    let id = match &frame {
        Frame::Execute(q) => Some(&q.review.operation_id),
        Frame::Reconcile(q) | Frame::Release(q) => Some(&q.operation_id),
        Frame::Prepare(q) => match &q.review.choice {
            Choice::AfterCommit { operation_id, .. } => Some(operation_id),
            _ => None,
        },
    };
    if error.is_none() {
        if let Some(id) = id {
            let op = c
                .review
                .feed
                .lock()
                .ok()
                .and_then(|f| f.records.get(id).cloned());
            if let Some(op) = op.filter(|op| {
                op.query.review.root == *frame.root() && op.published.load(Ordering::Acquire)
            }) {
                let claim = (|| {
                    op.disclosure_current()?;
                    if let Frame::Execute(q) = &frame {
                        if q.action != op.query.action
                            || serde_json::to_vec(q).map_err(denied)?.len() > COMMAND_BYTES
                        {
                            return Err(invalid());
                        }
                        let mut p = op.progress.lock().map_err(denied)?;
                        if let Some(previous) = &p.command {
                            if previous != q {
                                return Err(invalid());
                            }
                            op.budget()?;
                        } else {
                            op.write_current()?;
                            p.command = Some(q.clone());
                            initiator = true;
                        }
                    } else if let Frame::Prepare(q) = &frame {
                        let Choice::AfterCommit { capture_id, .. } = &q.review.choice else {
                            return Err(invalid());
                        };
                        op.budget()?;
                        op.claim_companion(capture_id)?;
                        op.companion
                            .as_ref()
                            .ok_or_else(unavailable)?
                            .lifetime
                            .retirement()
                            .native_link(&lifetime.as_ref().ok_or_else(unavailable)?.retirement())
                            .map_err(denied)?;
                    } else if matches!(frame, Frame::Reconcile(_)) {
                        op.budget()?;
                    }
                    Ok(())
                })();
                if let Err(e) = claim {
                    error = Some(matches!(e, Error::InvalidParams(_)));
                }
                if matches!(frame, Frame::Prepare(_)) {
                    predecessor = Some(op);
                } else {
                    target = Some(op);
                }
            } else if !matches!(frame, Frame::Release(_)) {
                error = Some(false);
            }
        }
    }
    let provider = predecessor
        .as_ref()
        .map(|p| p.metadata.provider.clone())
        .or_else(|| {
            matches!(frame, Frame::Prepare(_))
                .then(|| {
                    c.services
                        .gitlab_repository_connection_facts()
                        .ok()
                        .map(Arc::new)
                })
                .flatten()
        });
    let companion_reply = predecessor.is_some()
        || (initiator && target.as_ref().is_some_and(|op| op.companion.is_some()));
    let r = Arc::new_cyclic(|weak| Request {
        connection: c.weak.upgrade().expect("owned original connection"),
        weak: weak.clone(),
        frame,
        lifetime,
        subscriptions: Mutex::new(subscriptions),
        provider,
        target: Mutex::new(target),
        initiator,
        claimed: AtomicBool::new(false),
        completed: AtomicBool::new(false),
        consumed: AtomicBool::new(false),
        public: AtomicBool::new(false),
        entry_error: error,
        created: Instant::now(),
        admin_projection: AtomicBool::new(false),
        predecessor,
        companion_delivered: AtomicBool::new(false),
        companion_reply,
    });
    if let Some(life) = &r.lifetime {
        let retirement = life.retirement();
        let weak = Arc::downgrade(&r);
        let deadline = r
            .predecessor
            .as_ref()
            .map(|parent| (r.created + FRAME_TTL).min(parent.created + LEASE_TTL));
        tokio::spawn(async move {
            let timeout = async {
                if let Some(deadline) = deadline {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    tokio::time::sleep(FRAME_TTL).await;
                }
            };
            tokio::select! {()=retirement.native_cancelled()=>{},()=timeout=>{}}
            if let Some(r) = weak.upgrade() {
                if !r.started() {
                    r.finish();
                }
            }
        });
    }
    r
}
async fn checked<T>(r: &Arc<Request>, body: impl Future<Output = Result<T>> + Send) -> Result<T> {
    r.check()?;
    let retired = r.lifetime.as_ref().ok_or_else(unavailable)?.retirement();
    let f = async {
        tokio::select! {value=body=>value,()=retired.native_cancelled()=>Err(unavailable())}
    };
    tokio::pin!(f);
    std::future::poll_fn(|cx| {
        if let Err(e) = r.check() {
            return std::task::Poll::Ready(Err(e));
        }
        f.as_mut().poll(cx)
    })
    .await
}
fn entry<'a>(
    s: &'a Services,
    f: &Frame,
    body: impl FnOnce(Arc<Request>) -> BoxFuture<'static, Result<Value>> + Send + 'static,
) -> BoxFuture<'a, Result<Value>> {
    let r = Request::current(s, f);
    Box::pin(async move {
        let r = r?;
        let result = checked(&r, body(r.clone())).await;
        if result.is_err() {
            r.public.store(true, Ordering::Release);
            if r.initiator {
                if let Ok(op) = r.operation() {
                    op.retire();
                    if !op
                        .progress
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .started
                    {
                        op.complete();
                    }
                }
            }
        }
        result
    })
}
impl RepositoryReadRequestScope for Request {
    fn scope<'a>(&'a self, body: BoxFuture<'a, ()>) -> BoxFuture<'a, ()> {
        Box::pin(REVIEW_REQUEST.scope(self.weak.upgrade().expect("owned review frame"), body))
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
        // Reserve the opt-in original attempt before its future can await or be
        // overtaken. A second attempt cannot publish through guard unwinding.
        let claimed = !self.companion_reply || !self.consumed.swap(true, Ordering::AcqRel);
        Box::pin(async move {
            let result = async {
                if !claimed {
                    return Err(unavailable());
                }
                if self.public.load(Ordering::Acquire) || self.entry_error.is_some() {
                    if !self.companion_reply && self.consumed.swap(true, Ordering::AcqRel) {
                        return Err(unavailable());
                    }
                    return transfer();
                }
                entered?;
                let r = self.weak.upgrade().ok_or_else(unavailable)?;
                checked(&r, async {
                    let op = self.operation()?;
                    let _legacy = self
                        .connection
                        .caller
                        .legacy_lease()
                        .await
                        .map_err(denied)?;
                    op.disclosure_current()?;
                    op.metadata.validate(false).await?;
                    if self.admin_projection.load(Ordering::Acquire) {
                        Services::require_administrator("sourceControl.authStatus")
                            .map_err(denied)?;
                    }
                    let _locked = if matches!(self.frame, Frame::Prepare(_)) {
                        op.write_current()?;
                        op.metadata.validate(true).await?;
                        Some(final_prepare_lock(self.connection.clone(), op.clone()).await?)
                    } else {
                        None
                    };
                    self.check()?;
                    self.connection
                        .parent
                        .native_dispatch(|| {
                            self.lifetime
                                .as_ref()
                                .ok_or(AdmissionError::Retired)?
                                .retirement()
                                .native_dispatch(|| {
                                    op.disclosure.retirement().native_dispatch(|| {
                                        let action = || {
                                            if self.completed.load(Ordering::Acquire)
                                                || (!self.companion_reply
                                                    && self.consumed.swap(true, Ordering::AcqRel))
                                            {
                                                return Err(AdmissionError::Retired);
                                            }
                                            let mut transfer = || transfer().map_err(local);
                                            let result = if matches!(self.frame, Frame::Execute(_))
                                                && self.initiator
                                                && op.companion.is_some()
                                            {
                                                self.deliver_companion(&op, &mut transfer)
                                            } else if matches!(self.frame, Frame::Prepare(_))
                                                && self.predecessor.is_some()
                                            {
                                                self.publish_companion(&mut transfer)
                                            } else {
                                                transfer()
                                            };
                                            if result.is_ok()
                                                && matches!(self.frame, Frame::Prepare(_))
                                            {
                                                if let Some(published) = &op.companion_publication {
                                                    let _ = published.set(Instant::now());
                                                }
                                                op.published.store(true, Ordering::Release);
                                            }
                                            result
                                        };
                                        op.metadata.with_metadata(action)
                                    })
                                })
                        })
                        .map_err(denied)
                })
                .await
            }
            .await;
            if result.is_err() && self.companion_reply {
                // The once-reservation still excludes other deliveries while
                // all consuming guards unwind before terminal retirement.
                if let Ok(op) = self.operation() {
                    op.retire();
                }
                self.finish();
            }
            result
        })
    }
}

impl Request {
    fn capture_id(&self) -> Result<&str> {
        match &self.frame {
            Frame::Prepare(q) => match &q.review.choice {
                Choice::AfterCommit { capture_id, .. } => Ok(capture_id),
                _ => Err(invalid()),
            },
            _ => Err(invalid()),
        }
    }
    fn deliver_companion(
        &self,
        op: &Operation,
        transfer: &mut dyn FnMut() -> AdmissionResult<()>,
    ) -> AdmissionResult<()> {
        let companion = op.companion.as_ref().ok_or(AdmissionError::Retired)?;
        let mut called = false;
        let result = companion.lifetime.retirement().native_dispatch(|| {
            let mut state = companion.state.lock().map_err(local)?;
            called = true;
            let result = transfer();
            if result.is_ok()
                && state.normal
                && state.witness.is_some()
                && !state.closed
                && Instant::now() < op.created + LEASE_TTL
            {
                state.delivered = true;
                self.companion_delivered.store(true, Ordering::Release);
            } else if result.is_err() {
                state.closed = true;
            }
            result
        });
        // A retired intent does not erase an otherwise disclosable receipt.
        if called {
            result
        } else {
            transfer()
        }
    }
    fn publish_companion(
        &self,
        transfer: &mut dyn FnMut() -> AdmissionResult<()>,
    ) -> AdmissionResult<()> {
        let parent = self.predecessor.as_ref().ok_or(AdmissionError::Retired)?;
        let companion = parent.companion.as_ref().ok_or(AdmissionError::Retired)?;
        let capture = self.capture_id().map_err(local)?;
        companion.lifetime.retirement().native_dispatch(|| {
            let state = companion.state.lock().map_err(local)?;
            if state.closed
                || !state.normal
                || !state.delivered
                || Instant::now() >= parent.created + LEASE_TTL
                || state.capture.as_deref() != Some(capture)
            {
                return Err(AdmissionError::Retired);
            }
            transfer()
        })
    }
}

async fn authority(c: &Connection, root: &RepositoryRootId) -> Result<RepositoryAuthoritySnapshot> {
    c.entered().map_err(denied)?;
    let Caller::Wire {
        principal_id,
        host_role,
    } = c.caller.caller()
    else {
        return Err(unavailable());
    };
    let hash = match c.caller.wire_credential() {
        Some(WireCredential::Principal { token_hash, .. }) => Some(token_hash.as_str()),
        _ => None,
    };
    let before = c
        .services
        .store
        .repository_authority_snapshot(&root.workspace_id, principal_id, hash)
        .await
        .map_err(denied)?;
    c.services
        .require_member(&root.workspace_id)
        .await
        .map_err(denied)?;
    c.services
        .require_host_execution("accept-changes")
        .await
        .map_err(denied)?;
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
        .repository_authority_snapshot(&root.workspace_id, principal_id, hash)
        .await
        .map_err(denied)?;
    if before != after {
        return Err(unavailable());
    }
    c.caller
        .verify(
            &engine::RepositoryAuthorityFacts {
                caller: c.caller.caller().clone(),
                workspace: root.workspace_id.clone(),
                workspace_exists: before.workspace.value.is_some(),
                primary_principal_id: before
                    .primary_principal
                    .value
                    .as_ref()
                    .map(|p| p.id.clone()),
                workspace_role: before.workspace_grant.value,
                credential,
                provenance: engine::RepositoryAuthorityProvenance::Store(Box::new(before.clone())),
                internal_stages: vec![],
            },
            &root.workspace_id,
        )
        .map_err(denied)?;
    Ok(before)
}
fn stages(q: &Prepare) -> Result<Vec<Stage>> {
    if q.review.companion.is_some()
        && (q.action != Stage::Commit
            || q.files.is_some()
            || q.options != intent_core::repository_request::NativeReviewOptions::default()
            || q.review.push_remote.is_some()
            || q.review.target_branch.as_deref().is_none_or(str::is_empty)
            || matches!(q.review.choice, Choice::AfterCommit { .. }))
    {
        return Err(invalid());
    }
    if matches!(q.review.choice, Choice::AfterCommit { .. })
        && (q.action != Stage::CreatePr
            || q.files.is_some()
            || q.options != intent_core::repository_request::NativeReviewOptions::default()
            || q.review.target_branch.is_some()
            || q.review.push_remote.is_some())
    {
        return Err(invalid());
    }
    let mut plan = vec![q.action];
    if q.options.push_after_commit {
        if q.action != Stage::Commit {
            return Err(invalid());
        }
        plan.push(Stage::Push);
    }
    if q.options.create_pr_after_push {
        if q.action != Stage::Commit {
            return Err(invalid());
        }
        plan.push(Stage::CreatePr);
    }
    if q.action != Stage::Commit && (q.options.stage_unstaged || q.files.is_some()) {
        return Err(invalid());
    }
    if serde_json::to_vec(q).map_err(denied)?.len() > COMMAND_BYTES {
        return Err(invalid());
    }
    Ok(plan)
}
fn saved(snapshot: &RepositorySelectionSnapshot) -> Result<intent_core::SavedReviewSelection> {
    match snapshot.selection() {
        Some(RepositoryStoredSelection::NeverSaved | RepositoryStoredSelection::Reset) => {
            Ok(intent_core::SavedReviewSelection::Automatic)
        }
        Some(RepositoryStoredSelection::Saved(v)) => Ok(v.clone()),
        None => Err(unavailable()),
    }
}
fn explicit(q: &Prepare) -> Option<&RepositoryTarget> {
    match &q.review.choice {
        Choice::Saved | Choice::AfterCommit { .. } => None,
        Choice::ExplicitTarget { target } => Some(target),
    }
}
fn original(c: &Connection) -> AdmissionResult<OriginalRepositoryCaller> {
    OriginalRepositoryCaller::capture(if c.caller.wire_credential().is_some() {
        RepositoryEntry::Bearer
    } else {
        RepositoryEntry::AdmittedLocal
    })
}
fn fingerprint(path: &std::path::Path) -> Result<String> {
    use base64::Engine as _;
    use sha2::{Digest, Sha256};
    let repo = git2::Repository::open(path).map_err(denied)?;
    let mut index = repo.index().map_err(denied)?;
    index.read(true).map_err(denied)?;
    let mut hash = Sha256::new();
    for entry in index.iter() {
        hash.update(entry.mode.to_le_bytes());
        hash.update(entry.flags.to_le_bytes());
        hash.update(entry.flags_extended.to_le_bytes());
        hash.update(entry.id.as_bytes());
        hash.update(entry.path.len().to_le_bytes());
        hash.update(&entry.path);
    }
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hash.finalize()))
}
fn content_fingerprint(path: &std::path::Path, files: &[String]) -> Result<String> {
    use base64::Engine as _;
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    let mut total = 0;
    for file in files {
        let relative = std::path::Path::new(file);
        if relative
            .components()
            .any(|p| !matches!(p, std::path::Component::Normal(_)))
            || file.is_empty()
        {
            return Err(invalid());
        }
        hash.update(file.len().to_le_bytes());
        hash.update(file);
        match std::fs::symlink_metadata(path.join(file)) {
            Ok(meta) => {
                if !meta.is_file() || meta.len() > 64 * 1024 * 1024 {
                    return Err(invalid());
                }
                let bytes = std::fs::read(path.join(file)).map_err(denied)?;
                total += bytes.len();
                if total > 64 * 1024 * 1024 {
                    return Err(invalid());
                }
                hash.update([1]);
                hash.update(bytes);
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => hash.update([0]),
            Err(_) => return Err(unavailable()),
        }
    }
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hash.finalize()))
}

struct PreparationAuthority {
    metadata: Arc<CapturedMetadata>,
    retirement: RepositoryRetirement,
    request: RepositoryAuthorityRequest,
}
struct PreparationFence {
    metadata: Arc<CapturedMetadata>,
    retirement: RepositoryRetirement,
    parent: RepositoryRetirement,
    _legacy: Option<intent_core::caller::CredentialLease>,
}
impl RepositoryAuthority for PreparationAuthority {
    fn revalidate<'a>(
        &'a self,
        request: &'a RepositoryAuthorityRequest,
    ) -> CredentialFuture<'a, Box<dyn RepositoryAuthorityFence>> {
        Box::pin(async move {
            if request != &self.request || request.use_kind != Use::NativeRead {
                return Err(CredentialError::AuthorityDenied);
            }
            self.retirement.check_current().map_err(credential_error)?;
            let c = self
                .metadata
                .validate(true)
                .await
                .map_err(credential_error)?;
            let legacy = c.caller.legacy_lease().await.map_err(credential_error)?;
            Ok(Box::new(PreparationFence {
                metadata: self.metadata.clone(),
                retirement: self.retirement.clone(),
                parent: c.parent.clone(),
                _legacy: legacy,
            }) as Box<dyn RepositoryAuthorityFence>)
        })
    }
}
impl RepositoryAuthorityFence for PreparationFence {
    fn dispatch(
        self: Box<Self>,
        action: &mut (dyn FnMut() -> std::result::Result<(), CredentialError> + Send),
    ) -> std::result::Result<(), CredentialError> {
        let mut result = None;
        self.parent
            .native_dispatch(|| {
                self.retirement.native_dispatch(|| {
                    self.metadata.with_settings(|| {
                        result = Some(action());
                        Ok(())
                    })
                })
            })
            .map_err(credential_error)?;
        result.ok_or(CredentialError::AuthorityDenied)?
    }
}
struct Job {
    _connection: OwnedSemaphorePermit,
    _global: OwnedSemaphorePermit,
}
fn job(c: &Connection) -> Result<Job> {
    Ok(Job {
        _connection: c
            .review
            .workers
            .clone()
            .try_acquire_owned()
            .map_err(denied)?,
        _global: c
            .services
            .repository_review_capacity
            .workers
            .clone()
            .try_acquire_owned()
            .map_err(denied)?,
    })
}

pub(crate) fn prepare(s: &Services, q: Prepare) -> BoxFuture<'_, Result<Value>> {
    entry(s, &Frame::Prepare(q.clone()), move |r| {
        Box::pin(async move {
            stages(&q)?;
            let capacity = job(&r.connection)?;
            let c = r.connection.clone();
            let (tx, rx) = tokio::sync::oneshot::channel();
            let caller = c.caller.caller().clone();
            let wire = c.caller.wire_credential().cloned();
            tokio::spawn(with_caller(
                caller,
                with_wire_credential(wire, async move {
                    let _capacity = capacity;
                    let result = acquire(r, q).await;
                    let _ = tx.send(result);
                }),
            ));
            tokio::time::timeout(ACQUIRE, rx)
                .await
                .map_err(denied)?
                .map_err(denied)?
        })
    })
}
async fn acquire(r: Arc<Request>, q: Prepare) -> Result<Value> {
    let q = if let Some(parent) = &r.predecessor {
        parent.companion_witness(r.capture_id()?)?;
        parent.metadata.validate(true).await?;
        let mut inherited = parent.query.clone();
        inherited.action = Stage::CreatePr;
        inherited.review.companion = None;
        inherited
    } else {
        q
    };
    let c = &r.connection;
    let permit = c
        .review
        .records
        .clone()
        .try_acquire_owned()
        .map_err(denied)?;
    let global = c
        .services
        .repository_review_capacity
        .records
        .clone()
        .try_acquire_owned()
        .map_err(denied)?;
    r.check()?;
    let initial_authority = authority(c, &q.review.root).await?;
    let root = RootRecord::read(&c.services.store, &q.review.root)
        .await
        .map_err(denied)?;
    let selection = c
        .services
        .store
        .repository_selection_snapshot(&q.review.root)
        .await
        .map_err(denied)?;
    if selection.binding().is_none() || selection.root_incarnation().is_none() {
        return Err(unavailable());
    }
    let provider = r.provider.clone().ok_or_else(unavailable)?;
    provider.settled().ok_or_else(unavailable)?;
    let mut metadata = Arc::new(CapturedMetadata {
        connection: c.weak.clone(),
        root: root.clone(),
        selection,
        authority: initial_authority,
        provider,
        settings: c
            .services
            .settings_registry
            .as_ref()
            .map(|s| (s.clone(), s.snapshot())),
    });
    if let Some(parent) = &r.predecessor {
        parent.metadata.validate(true).await?;
        if metadata.root != parent.metadata.root
            || metadata.authority != parent.metadata.authority
            || metadata.selection.binding() != parent.metadata.selection.binding()
            || metadata.selection.root_incarnation() != parent.metadata.selection.root_incarnation()
            || metadata.selection.selection_revision()
                != parent.metadata.selection.selection_revision()
            || metadata.selection.selection() != parent.metadata.selection.selection()
            || !Arc::ptr_eq(&metadata.provider, &parent.metadata.provider)
        {
            return Err(unavailable());
        }
        metadata = parent.metadata.clone();
    }
    let life = r.lifetime.as_ref().ok_or_else(unavailable)?;
    let extra = life
        .subscribe(
            &c.services.store,
            c.caller.caller(),
            &[RepositoryLifecycleKey::Database, root.lifecycle_key()],
        )
        .map_err(denied)?;
    let (local_life, registration) = c.new_lifetime().map_err(denied)?;
    let retirement = local_life.retirement();
    life.retirement().native_link(&retirement).map_err(denied)?;
    r.subscriptions.lock().map_err(denied)?.push(extra);
    let _registration = registration;
    let acquired = AtomicBool::new(false);
    let read = RepositoryGitSource::with_group(
        &c.services.store,
        &c.services.worktree_locks,
        vec![root.clone()],
        retirement.clone(),
        |_| {
            acquired.store(true, Ordering::Release);
            let metadata = metadata.clone();
            let q = q.clone();
            async move {
                tokio::task::spawn_blocking(move || {
                    let (context, observed) = read_context_root_with_resolver(
                        &q.review.root,
                        metadata.root.path(),
                        &saved(&metadata.selection).map_err(local)?,
                        explicit(&q),
                        &resolver(Some(&metadata.provider))?,
                        &GitConfigEnvironment::default(),
                        |target| target_context(target, Some(&metadata.provider)),
                    )
                    .map_err(local)?;
                    let files = match &q.files {
                        Some(files) if !files.is_empty() => files.clone(),
                        _ if q.options.stage_unstaged => {
                            intent_git::commit::all_changed_paths(metadata.root.path())
                                .map_err(local)?
                        }
                        _ => Vec::new(),
                    };
                    let worktree_digest =
                        content_fingerprint(metadata.root.path(), &files).map_err(local)?;
                    let staging = fingerprint(metadata.root.path()).map_err(local)?;
                    let response =
                        crate::accept_changes::build_native_prepare_value(metadata.root.path(), &q)
                            .map_err(local)?;
                    Ok((context, observed, staging, files, worktree_digest, response))
                })
                .await
                .map_err(local)?
            }
        },
    );
    tokio::pin!(read);
    let result = tokio::select! {
        result = &mut read => result,
        () = retirement.native_cancelled() => {
            if acquired.load(Ordering::Acquire) { read.await } else { Err(AdmissionError::Retired) }
        },
        () = tokio::time::sleep(ACQUIRE) => {
            retirement.end_scope();
            if acquired.load(Ordering::Acquire) { read.await } else { Err(AdmissionError::Retired) }
        }
    };
    let (context, observed, staging, files, worktree_digest, mut response) =
        result.map_err(denied)?;
    r.check()?;
    metadata.validate(true).await?;
    if let Some(parent) = &r.predecessor {
        let witness = parent.companion_witness(r.capture_id()?)?;
        if observed != witness.observed || staging != witness.staging || !files.is_empty() {
            return Err(unavailable());
        }
    }
    let intent_core::ReviewSelectionOutcome::Resolved { target, .. } =
        &context.review_selection.outcome
    else {
        return Err(invalid());
    };
    if target.provider != RepositoryProvider::Gitlab {
        return Err(invalid());
    }
    let settled = metadata.provider.settled().ok_or_else(unavailable)?;
    let selected = settled.selected();
    let descriptor = settled.descriptor();
    if target.instance_base_url != descriptor.instance().as_str() {
        return Err(invalid());
    }
    let plan = stages(&q)?;
    let source = observed
        .private_root
        .source_ref
        .clone()
        .filter(|s| s.starts_with("refs/heads/"))
        .ok_or_else(invalid)?;
    let source_branch = source
        .strip_prefix("refs/heads/")
        .ok_or_else(invalid)?
        .to_string();
    let target_branch = q
        .review
        .target_branch
        .clone()
        .unwrap_or_else(|| source_branch.clone());
    if (plan.contains(&Stage::CreatePr) || q.review.companion.is_some())
        && (target_branch == source_branch
            || !git2::Reference::is_valid_name(&format!("refs/heads/{target_branch}")))
    {
        return Err(invalid());
    }
    let remote = if let Some(name) = &q.review.push_remote {
        observed
            .private_root
            .remotes
            .iter()
            .find(|remote| &remote.name == name)
    } else if plan.contains(&Stage::Push) {
        return Err(invalid());
    } else {
        let mut found=observed.private_root.remotes.iter().filter(|remote|context.remotes.iter().find(|r|r.name==remote.name).is_some_and(|r|r.fetch.iter().any(|endpoint|matches!(&endpoint.resolution,intent_core::RepositoryEndpointResolution::Resolved{target:t} if t==target))));
        let first = found.next();
        if found.next().is_some() {
            return Err(invalid());
        }
        first
    };
    if !observed.private_root.remotes.is_empty() && remote.is_none() {
        return Err(invalid());
    }
    let (transport, fetch, push) = if let Some(remote) = remote {
        let display = context
            .remotes
            .iter()
            .find(|display| display.name == remote.name)
            .ok_or_else(invalid)?;
        (
            Some(intent_core::NativeReviewTransport {
                remote_name: remote.name.clone(),
                fetch_urls: display.fetch.iter().map(|e| e.url.clone()).collect(),
                push_urls: display.push.iter().map(|e| e.url.clone()).collect(),
            }),
            remote.fetch.clone(),
            remote.push.clone(),
        )
    } else {
        (None, vec![], vec![])
    };
    if plan.contains(&Stage::Push) {
        intent_git::native_push::PreparedNativePush::prepare(
            root.path(),
            &transport.as_ref().ok_or_else(invalid)?.remote_name,
            &source,
            context.head_sha.as_deref().ok_or_else(invalid)?,
            &fetch,
            &push,
        )
        .map_err(|_| invalid())?;
    }
    let id = uuid::Uuid::new_v4().to_string();
    let scope = intent_core::ExecutionScope {
        daemon_id: c.services.daemon_boot_id.clone(),
        authority_scope_id: id.clone(),
        authority_generation: metadata
            .authority
            .workspace
            .revision
            .ok_or_else(unavailable)?
            .get(),
    };
    let read_request = RepositoryAuthorityRequest {
        execution: scope.clone(),
        target: target.clone(),
        connection: selected.binding.scope.clone(),
        use_kind: Use::NativeRead,
        allowed_transport: RepositoryCredentialTransport::GitlabApi(descriptor.clone()),
    };
    let read_authority = Arc::new(PreparationAuthority {
        metadata: metadata.clone(),
        retirement: life.retirement(),
        request: read_request.clone(),
    });
    let directory = c.services.repository_connection_directory();
    let read_admission = directory
        .admit(&selected.binding, read_request, read_authority)
        .map_err(denied)?;
    let provider = BoundGitlabRequestCredentials::new(
        directory.clone(),
        read_admission,
        c.services
            .gitlab_repository_secret_reader()
            .map_err(denied)?,
        ACQUIRE,
    )
    .map_err(denied)?
    .into_provider()
    .map_err(denied)?;
    let repo = repo_ref(target)?;
    let (project_id, path) = provider
        .confirmed_project_identity(&repo)
        .await
        .map_err(denied)?;
    if path != target.project_path {
        return Err(invalid());
    }
    if plan.contains(&Stage::CreatePr) || q.review.companion.is_some() {
        let branch = remote_branch(&provider, &repo, &target_branch).await?;
        if branch.is_none() {
            return Err(invalid());
        }
    }
    let branch = |name: String| intent_core::NativeReviewBranchTarget {
        repository: target.clone(),
        provider_project_id: Some(project_id.to_string()),
        connection: Some(selected.binding.scope.clone()),
        branch: name,
    };
    let preparation = Preparation {
        operation_id: id.clone(),
        scope: scope.clone(),
        context_revision: intent_core::RepositoryContextRevision::new(id.clone(), 1),
        root: q.review.root.clone(),
        worktree_id: uuid::Uuid::new_v4().to_string(),
        source: branch(source_branch),
        target: branch(target_branch),
        local_head_sha: context.head_sha.clone(),
        transport,
    };
    let mut credential_requests = Vec::new();
    for stage in &plan {
        let (use_kind, allowed_transport) = match stage {
            Stage::Commit => continue,
            Stage::Push => (
                Use::NativePush,
                RepositoryCredentialTransport::GitHttps(push.clone()),
            ),
            Stage::CreatePr => (
                Use::NativeReviewCreate,
                RepositoryCredentialTransport::GitlabApi(descriptor.clone()),
            ),
        };
        let request = RepositoryAuthorityRequest {
            execution: scope.clone(),
            target: target.clone(),
            connection: selected.binding.scope.clone(),
            use_kind,
            allowed_transport,
        };
        // P validates all approved destinations; no secret or stage authority is released here.
        let check = Arc::new(PreparationAuthority {
            metadata: metadata.clone(),
            retirement: life.retirement(),
            request: request.clone(),
        });
        directory
            .admit(&selected.binding, request.clone(), check)
            .map_err(denied)?;
        credential_requests.push(request);
    }
    let facts = RepositoryOperationFacts {
        preparation,
        worktree_path: root.path().to_path_buf(),
        git_dir: observed.change_inputs.git_dir.clone(),
        common_dir: observed.change_inputs.common_dir.clone(),
        source_ref: source,
        staging_fingerprint: Some(staging),
        fetch_destinations: fetch,
        push_destinations: push,
        credential_requests,
    };
    if let Some(parent) = &r.predecessor {
        parent.companion_witness(r.capture_id()?)?;
        let original = &parent.facts;
        if facts.worktree_path != original.worktree_path
            || facts.git_dir != original.git_dir
            || facts.common_dir != original.common_dir
            || facts.source_ref != original.source_ref
            || facts.fetch_destinations != original.fetch_destinations
            || facts.push_destinations != original.push_destinations
            || facts.preparation.source != original.preparation.source
            || facts.preparation.target != original.preparation.target
            || facts.preparation.transport != original.preparation.transport
        {
            return Err(unavailable());
        }
    }
    metadata.validate(true).await?;
    r.check()?;
    let (write, write_sub) = c.new_lifetime().map_err(denied)?;
    let (disclosure, disclosure_sub) = c.new_lifetime().map_err(denied)?;
    let mut write_keys = keys(&q.review.root);
    write_keys.push(root.lifecycle_key());
    let mut disclosure_keys = keys(&q.review.root);
    disclosure_keys.retain(|k| !matches!(k, RepositoryLifecycleKey::Selection { .. }));
    let mut subscriptions = vec![
        write_sub,
        disclosure_sub,
        write
            .subscribe(&c.services.store, c.caller.caller(), &write_keys)
            .map_err(denied)?,
        disclosure
            .subscribe(&c.services.store, c.caller.caller(), &disclosure_keys)
            .map_err(denied)?,
    ];
    let companion = if q.review.companion.is_some() {
        let (lifetime, registration) = c.new_lifetime().map_err(denied)?;
        subscriptions.push(registration);
        subscriptions.push(
            lifetime
                .subscribe(&c.services.store, c.caller.caller(), &write_keys)
                .map_err(denied)?,
        );
        Some(Companion {
            lifetime,
            state: Mutex::default(),
        })
    } else {
        None
    };
    if let Some(parent) = &r.predecessor {
        parent
            .companion
            .as_ref()
            .ok_or_else(unavailable)?
            .lifetime
            .retirement()
            .native_link(&write.retirement())
            .map_err(denied)?;
    }
    let op = Arc::new(Operation {
        id: id.clone(),
        query: q.clone(),
        metadata: metadata.clone(),
        facts,
        observed,
        files,
        content_fingerprint: worktree_digest,
        write,
        disclosure,
        _subscriptions: subscriptions,
        _record: permit,
        _global: global,
        created: Instant::now(),
        published: AtomicBool::new(false),
        noticed: AtomicBool::new(false),
        observations: AtomicUsize::new(0),
        progress: Mutex::default(),
        changed: Notify::new(),
        companion,
        companion_publication: r.predecessor.as_ref().map(|_| std::sync::OnceLock::new()),
    });
    let admin = Services::require_administrator("sourceControl.authStatus").is_ok();
    r.admin_projection.store(admin, Ordering::Release);
    response["reviewPreparation"] = project(&op.facts.preparation, admin);
    let sequence = c.review.feed.lock().map_err(denied)?.sequence;
    response["reviewOperation"] = json!(NativeReviewOperationCapture {
        operation_id: id.clone(),
        root: op.query.review.root.clone(),
        retirement_sequence: sequence.to_string(),
        expires_after_ms: 300_000
    });
    life.retirement()
        .native_dispatch(|| {
            if r.completed.load(Ordering::Acquire) {
                return Err(AdmissionError::Retired);
            }
            *r.target.lock().map_err(local)? = Some(op.clone());
            c.review
                .feed
                .lock()
                .map_err(local)?
                .records
                .insert(id, op.clone());
            Ok(())
        })
        .map_err(denied)?;
    monitor(&op);
    Ok(response)
}
fn repo_ref(target: &RepositoryTarget) -> Result<intent_core::RepoRef> {
    let (owner, name) = target
        .project_path
        .rsplit_once('/')
        .filter(|(a, b)| !a.is_empty() && !b.is_empty())
        .ok_or_else(invalid)?;
    Ok(intent_core::RepoRef::new(owner, name))
}
async fn remote_branch(
    provider: &GitLabSourceControl,
    repo: &intent_core::RepoRef,
    branch: &str,
) -> Result<Option<String>> {
    let mut page = intent_sourcecontrol::PageParams::first(100);
    let mut found = None;
    let mut cursors = std::collections::HashSet::new();
    for _ in 0..10 {
        let result = provider
            .list_remote_branches(&repo.owner, &repo.name, Some(branch), page.clone())
            .await
            .map_err(denied)?;
        for item in result.items {
            if item.name == branch {
                if found.is_some() {
                    return Err(unavailable());
                }
                found = Some(
                    item.commit_sha
                        .filter(|s| !s.is_empty())
                        .ok_or_else(unavailable)?,
                );
            }
        }
        match result.next_cursor {
            Some(cursor) if cursors.insert(cursor.clone()) => page.cursor = Some(cursor),
            Some(_) => return Err(unavailable()),
            None => return Ok(found),
        }
    }
    Err(unavailable())
}
fn monitor(op: &Arc<Operation>) {
    let weak = Arc::downgrade(op);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let Some(op) = weak.upgrade() else {
                return;
            };
            if op.write_current().is_err() && !op.companion_normal() {
                op.retire();
            } else {
                let metadata = op.metadata.clone();
                // One observation per retained operation, awaited to real completion.
                // The original record permits remain held by `op` throughout. No R
                // fence or settings guard crosses into the blocking P observation.
                let current = tokio::task::spawn_blocking(move || {
                    metadata.with_settings(|| Ok(()))?;
                    metadata
                        .provider
                        .native_review_metadata_current()
                        .map_err(local)
                })
                .await;
                if !matches!(current, Ok(Ok(true))) {
                    op.retire();
                }
            }
            let expired = op.disclosure_current().is_err();
            let busy = {
                let p = op
                    .progress
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                p.started && p.settled.is_none()
            };
            if expired && !busy {
                op.retire();
                op.disclosure.retirement().end_scope();
                if let Some(c) = op.metadata.connection.upgrade() {
                    c.review
                        .feed
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .records
                        .remove(&op.id);
                }
                return;
            }
        }
    });
}

pub(crate) fn execute(s: &Services, q: Execute) -> BoxFuture<'_, Result<Value>> {
    entry(s, &Frame::Execute(q.clone()), move |r| {
        Box::pin(async move {
            let op = r.operation()?;
            if r.initiator {
                if op.query.action == Stage::Commit
                    && q.commit_message
                        .as_deref()
                        .is_none_or(|s| s.trim().is_empty())
                {
                    return Err(invalid());
                }
                op.metadata.validate(true).await?;
                op.write_current()?;
                r.check()?;
                let capacity = job(&r.connection)?;
                {
                    let mut p = op.progress.lock().map_err(denied)?;
                    if p.started || p.settled.is_some() {
                        return Err(unavailable());
                    }
                    p.started = true;
                }
                let c = r.connection.clone();
                let owned = op.clone();
                let queue_deadline = r.created + FRAME_TTL;
                tokio::spawn(with_caller(
                    c.caller.caller().clone(),
                    with_wire_credential(c.caller.wire_credential().cloned(), async move {
                        let mut companion_finish =
                            owned.companion.as_ref().map(|_| CompanionCompletion {
                                operation: owned.clone(),
                                capacity: None,
                                normal: false,
                            });
                        let _capacity = if let Some(finish) = &mut companion_finish {
                            finish.capacity = Some(capacity);
                            None
                        } else {
                            Some(capacity)
                        };
                        let _finish = owned.companion.is_none().then(|| Completion(owned.clone()));
                        let result = run(&c, &owned, &q, queue_deadline).await;
                        if let Some(finish) = &mut companion_finish {
                            finish.normal = result.is_ok();
                        }
                        if result.is_err() {
                            if let Ok(p) = owned.progress.lock() {
                                if let Some(engine) = &p.engine {
                                    if let Ok(e) = engine.execution() {
                                        drop(p);
                                        owned.retain(e);
                                    }
                                }
                            }
                        }
                    }),
                ));
            }
            let deadline = tokio::time::sleep(STAGE_TTL * 3);
            tokio::pin!(deadline);
            loop {
                let changed = op.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if op.progress.lock().map_err(denied)?.settled.is_some() {
                    break;
                }
                tokio::select! {()=&mut changed=>{},()=&mut deadline=>break}
            }
            let admin = Services::require_administrator("sourceControl.authStatus").is_ok();
            r.admin_projection.store(admin, Ordering::Release);
            let state = op.state(admin)?;
            Ok(execute_response(state))
        })
    })
}
// Declared before the ordinary guard, and releases its own permits before
// settlement is visible. Unwinding never promotes the private success witness.
struct CompanionCompletion {
    operation: Arc<Operation>,
    capacity: Option<Job>,
    normal: bool,
}
impl Drop for CompanionCompletion {
    fn drop(&mut self) {
        drop(self.capacity.take());
        self.operation.complete_owned(self.normal);
    }
}
struct Completion(Arc<Operation>);
impl Drop for Completion {
    fn drop(&mut self) {
        self.0.complete();
    }
}
fn execute_response(mut state: Value) -> Value {
    use crate::accept_changes::step;
    let mut steps = Vec::new();
    if let Some(execution) = state.get("reviewExecution").cloned() {
        let bad = matches!(
            execution["outcome"]["status"].as_str(),
            Some("failed" | "uncertain")
        );
        let mut result = serde_json::Map::new();
        for effect in execution["gitReceipts"].as_array().into_iter().flatten() {
            if let Some(hash) = effect.get("commitHash") {
                result.insert("commitHash".into(), hash.clone());
                steps.push(step("commit", "Commit", "completed", hash.as_str(), None));
            }
            if let Some(sha) = effect.get("pushedSha") {
                result.insert("pushedSha".into(), sha.clone());
                steps.push(step("push", "Push", "completed", sha.as_str(), None));
            }
        }
        if matches!(
            execution["outcome"]["status"].as_str(),
            Some("created" | "reused")
        ) {
            let review = &execution["outcome"]["review"];
            result.insert("prNumber".into(), review["resource"]["number"].clone());
            result.insert("prUrl".into(), review["url"].clone());
            steps.push(step(
                "create-pr",
                "Create review",
                "completed",
                review["url"].as_str(),
                None,
            ));
        }
        if bad {
            let message = execution["outcome"]["message"]
                .as_str()
                .unwrap_or("Repository review did not complete");
            let failed_stage = execution["outcome"]["stage"]
                .as_str()
                .unwrap_or("create-pr");
            steps.push(step(
                failed_stage,
                failed_stage,
                "failed",
                None,
                Some(message),
            ));
            state["error"] = json!(message);
        }
        state["success"] = json!(!bad);
        state["result"] = json!(result);
    } else {
        state["success"] = json!(false);
        state["error"] =
            json!("Repository review outcome is pending; reconcile the original socket");
    }
    state["steps"] = json!(steps);
    state
}

pub(crate) fn reconcile(s: &Services, q: Bound) -> BoxFuture<'_, Result<Value>> {
    entry(s, &Frame::Reconcile(q), |r| {
        Box::pin(async move {
            let op = r.operation()?;
            op.metadata.validate(false).await?;
            op.disclosure_current()?;
            let admin = Services::require_administrator("sourceControl.authStatus").is_ok();
            r.admin_projection.store(admin, Ordering::Release);
            op.state(admin)
        })
    })
}
pub(crate) fn release(s: &Services, q: Bound) -> BoxFuture<'_, Result<Value>> {
    entry(s, &Frame::Release(q), |r| {
        Box::pin(async move {
            if let Ok(op) = r.operation() {
                op.metadata.validate(false).await?;
                op.retire();
            } else {
                authority(&r.connection, r.frame.root()).await?;
            }
            r.public.store(true, Ordering::Release);
            Ok(json!({"released":true}))
        })
    })
}
// Queue admission ends at the first successful synchronous stage claim, not at
// source observation (which can still wait for a worker or branch-pair lock).
// Serialize that claim with expiry; release this mutex before any retirement,
// so expiry never holds it while joining the original R/P admission fences.
struct StageQueue {
    deadline: Instant,
    admitted: Mutex<bool>,
}
impl StageQueue {
    fn claim<T>(&self, claim: impl FnOnce() -> AdmissionResult<T>) -> AdmissionResult<T> {
        let mut admitted = self.admitted.lock().map_err(|_| AdmissionError::Retired)?;
        if !*admitted && Instant::now() >= self.deadline {
            return Err(AdmissionError::Retired);
        }
        let result = claim()?;
        *admitted = true;
        Ok(result)
    }

    fn expired_unadmitted(&self) -> bool {
        self.admitted.lock().map_or(true, |admitted| !*admitted)
    }
}

async fn run(
    c: &Arc<Connection>,
    op: &Arc<Operation>,
    command: &Execute,
    queue_deadline: Instant,
) -> Result<()> {
    op.write_current()?;
    op.metadata.validate(true).await?;
    let (life, registration) = c.new_lifetime().map_err(denied)?;
    let retirement = life.retirement();
    op.write
        .retirement()
        .native_link(&retirement)
        .map_err(denied)?;
    let _registration = registration;
    let context = RepositoryContextInput {
        scope: op.facts.preparation.scope.clone(),
        revision: op.facts.preparation.context_revision.clone(),
        roots: vec![AdmittedRepositoryRoot {
            root: op.query.review.root.clone(),
            path: op.metadata.root.path().to_path_buf(),
            saved_selection: saved(&op.metadata.selection)?,
            explicit_target: explicit(&op.query).cloned(),
            targets: vec![target_context(
                &op.facts.preparation.source.repository,
                Some(&op.metadata.provider),
            )],
        }],
    };
    let input = RepositorySourceInput {
        facts: op.facts.clone(),
        context,
        resolver: resolver(Some(&op.metadata.provider)).map_err(denied)?,
        environment: GitConfigEnvironment::default(),
        #[cfg(test)]
        before_lock: None,
    };
    let queue = Arc::new(StageQueue {
        deadline: queue_deadline,
        admitted: Mutex::new(false),
    });
    let observation_entered = AtomicBool::new(false);
    let operation = with_repository_lifecycle_source_observed(
        &c.services,
        original(c).map_err(denied)?,
        op.id.clone(),
        stages(&op.query)?,
        input,
        (life, &observation_entered),
        |admission| {
            let op = op.clone();
            let c = c.clone();
            let command = command.clone();
            let queue = queue.clone();
            async move {
                op.write_current().map_err(local)?;
                op.metadata.validate(true).await.map_err(local)?;
                op.progress.lock().map_err(local)?.engine = Some(admission.clone());
                let runtime = tokio::runtime::Handle::current();
                let caller = c.caller.caller().clone();
                let wire = c.caller.wire_credential().cloned();
                // The actual blocking worker exists before ANY stage admission. It
                // retains the shared worktree lock and capacity through real completion.
                tokio::task::spawn_blocking(move || {
                    runtime.block_on(with_caller(
                        caller,
                        with_wire_credential(
                            wire,
                            run_stages(&c, &op, &command, &admission, &queue),
                        ),
                    ))
                })
                .await
                .map_err(local)?
                .map_err(local)
            }
        },
    );
    tokio::pin!(operation);
    let cancelled = op.write.retirement();
    // This task is the only source poller. No poll can occur between a false
    // observation-entry check and dropping the pinned future on return.
    tokio::select! {
        biased;
        () = cancelled.native_cancelled() => {
            if observation_entered.load(Ordering::Acquire) {
                operation.await.map_err(denied)
            } else {
                Err(unavailable())
            }
        }
        () = tokio::time::sleep_until(queue_deadline) => {
            // An expired unentered queue wins even if source polling is ready.
            // If a worker is already running, StageQueue::claim also checks the
            // fixed deadline under the same mutex, before its first claim.
            if queue.expired_unadmitted() {
                op.retire();
            }
            if observation_entered.load(Ordering::Acquire) {
                operation.await.map_err(denied)
            } else {
                Err(unavailable())
            }
        }
        result = &mut operation => result.map_err(denied),
    }
}
// Before a consuming stamp exists, every exit must classify the actual pending
// stage in the original engine. Its existing contract preserves earlier receipts
// and refuses to overwrite an active or already terminal stage. After admission,
// the stamp alone owns completion/uncertainty, including unwinding and cancellation.
struct UnadmittedStage<'a> {
    operation: &'a Operation,
    admission: &'a RepositoryOperationAdmission,
    stage: Option<Stage>,
}
impl Drop for UnadmittedStage<'_> {
    fn drop(&mut self) {
        if let Some(stage) = self.stage {
            if let Ok(execution) = self
                .admission
                .fail_before_dispatch(stage, AdmissionError::Retired)
            {
                self.operation.retain(execution);
            }
        }
    }
}

fn validate_companion_git(op: &Operation) -> Result<()> {
    let (_, observed) = read_context_root_with_resolver(
        &op.query.review.root,
        op.metadata.root.path(),
        &saved(&op.metadata.selection)?,
        explicit(&op.query),
        &resolver(Some(&op.metadata.provider)).map_err(denied)?,
        &GitConfigEnvironment::default(),
        |target| target_context(target, Some(&op.metadata.provider)),
    )
    .map_err(denied)?;
    if observed != op.observed
        || Some(fingerprint(op.metadata.root.path())?) != op.facts.staging_fingerprint
    {
        return Err(unavailable());
    }
    Ok(())
}

fn commit_witness(op: &Operation, sha: &str, staging: Option<&str>) -> Result<CommitWitness> {
    let staging = staging
        .filter(|s| Some(*s) == op.facts.staging_fingerprint.as_deref())
        .ok_or_else(unavailable)?;
    let (_, observed) = read_context_root_with_resolver(
        &op.query.review.root,
        op.metadata.root.path(),
        &saved(&op.metadata.selection)?,
        explicit(&op.query),
        &resolver(Some(&op.metadata.provider)).map_err(denied)?,
        &GitConfigEnvironment::default(),
        |target| target_context(target, Some(&op.metadata.provider)),
    )
    .map_err(denied)?;
    let mut expected = op.observed.clone();
    expected.head_sha = Some(sha.to_owned());
    // The separate digest fixes the configuration prefix. Only the witnessed
    // commit's HEAD contribution may change in the existing mixed fingerprint.
    expected
        .change_inputs
        .fingerprint
        .clone_from(&observed.change_inputs.fingerprint);
    if expected != observed {
        return Err(unavailable());
    }
    Ok(CommitWitness {
        observed,
        staging: staging.to_owned(),
    })
}

async fn run_stages(
    c: &Connection,
    op: &Arc<Operation>,
    command: &Execute,
    admission: &RepositoryOperationAdmission,
    queue: &StageQueue,
) -> Result<()> {
    let mut pending = UnadmittedStage {
        operation: op,
        admission,
        stage: Some(op.query.action),
    };
    if content_fingerprint(op.metadata.root.path(), &op.files)? != op.content_fingerprint {
        return Err(unavailable());
    }
    let plan = stages(&op.query)?;
    for stage in plan {
        pending.stage = Some(stage);
        let preflight = async {
            op.write_current()?;
            op.metadata.validate(true).await?;
            engine::revalidate_repository_stage(admission, stage)
                .await
                .map_err(denied)
        }
        .await;
        let Ok(checked) = preflight else {
            op.retain(
                admission
                    .fail_before_dispatch(stage, AdmissionError::Retired)
                    .map_err(denied)?,
            );
            pending.stage = None;
            break;
        };
        // Allocate everything before the consuming comparison. Branch-pair
        // serialization is Services-owned, keyed by the full private identity.
        let create_lock = if stage == Stage::CreatePr {
            Some(create_lock(c, op)?)
        } else {
            None
        };
        let _create = if let Some(lock) = create_lock {
            Some(
                tokio::time::timeout(FRAME_TTL, lock.lock_owned())
                    .await
                    .map_err(denied)?,
            )
        } else {
            None
        };
        let prepared_push = if stage == Stage::Push {
            let state = admission.execution().map_err(denied)?;
            let sha = state
                .git_receipts
                .iter()
                .rev()
                .find_map(|r| {
                    if let GitReceipt::Commit { commit_hash } = r {
                        Some(commit_hash.as_str())
                    } else {
                        None
                    }
                })
                .or(op.facts.preparation.local_head_sha.as_deref())
                .ok_or_else(unavailable)?;
            Some(intent_git::native_push::PreparedNativePush::prepare(
                op.metadata.root.path(),
                &op.facts
                    .preparation
                    .transport
                    .as_ref()
                    .ok_or_else(unavailable)?
                    .remote_name,
                &op.facts.source_ref,
                sha,
                &op.facts.fetch_destinations,
                &op.facts.push_destinations,
            )?)
        } else {
            None
        };
        // A lock wait requires a new check; the first check is never a queued grant.
        let checked = if stage == Stage::CreatePr {
            drop(checked);
            engine::revalidate_repository_stage(admission, stage)
                .await
                .map_err(denied)?
        } else {
            checked
        };
        if op.companion.is_some() || op.companion_publication.is_some() {
            // The shared facts projection intentionally does not include every
            // config/inventory observation. Opt-in continuity compares the full
            // private snapshot in the original blocking worker/worktree lock,
            // before entering either synchronous consuming fence. No I/O or
            // extra queue is introduced into the R/P comparison itself.
            validate_companion_git(op)?;
        }
        let stamp = engine::begin_native_repository_stage(checked, |claim| {
            op.metadata.with_metadata(|| queue.claim(claim))
        })
        .map_err(denied)?;
        pending.stage = None;
        let alarm = op.write.retirement();
        let _timer = AbortOnDrop(tokio::spawn(async move {
            tokio::time::sleep(STAGE_TTL).await;
            alarm.end_scope();
        }));
        let result = match stage {
            Stage::Commit => {
                let outcome = (|| {
                    if !op.files.is_empty() {
                        intent_git::stage::stage(op.metadata.root.path(), &op.files)?;
                    }
                    intent_git::commit::commit_observed(
                        op.metadata.root.path(),
                        command.commit_message.as_deref().ok_or_else(invalid)?,
                        |sha| {
                            op.primitive(GitReceipt::Commit {
                                commit_hash: sha.into(),
                            });
                        },
                    )
                })();
                if let Ok(outcome) = outcome {
                    op.primitive(GitReceipt::Commit {
                        commit_hash: outcome.hash.clone(),
                    });
                    let after = fingerprint(op.metadata.root.path()).ok();
                    let witness = if op.companion.is_some() {
                        commit_witness(op, &outcome.hash, after.as_deref()).ok()
                    } else {
                        None
                    };
                    let execution = engine::classify_repository_completion(
                        stamp,
                        RepositoryCompletion::Committed {
                            hash: outcome.hash,
                            staging_after: after,
                        },
                    )
                    .map_err(denied)?;
                    op.retain(execution);
                    if let Some(companion) = &op.companion {
                        companion.state.lock().map_err(denied)?.witness = witness;
                    }
                    // Attribution follows the retained primitive receipt.
                    for path in outcome.files {
                        let key = crate::file_tracking::normalize_path(&path);
                        let staged_failed = c
                            .services
                            .store
                            .set_tracked_change_stage(
                                &op.query.workspace_id,
                                &key,
                                "staged",
                                "committed",
                            )
                            .await
                            .is_err();
                        let unstaged_failed = c
                            .services
                            .store
                            .set_tracked_change_stage(
                                &op.query.workspace_id,
                                &key,
                                "unstaged",
                                "committed",
                            )
                            .await
                            .is_err();
                        if staged_failed || unstaged_failed {
                            if let Some(companion) = &op.companion {
                                companion.state.lock().map_err(denied)?.witness = None;
                            }
                        }
                    }
                    true
                } else {
                    op.retain(
                        engine::classify_repository_completion(
                            stamp,
                            RepositoryCompletion::Uncertain {
                                message: "Git commit completion could not be confirmed".into(),
                            },
                        )
                        .map_err(denied)?,
                    );
                    false
                }
            }
            Stage::Push => {
                let (request, authority) = stamp.credential_authority().map_err(denied)?;
                let directory = c.services.repository_connection_directory();
                let settled = op.metadata.provider.settled().ok_or_else(unavailable)?;
                let granted = directory
                    .admit(&settled.selected().binding, request, authority)
                    .map_err(denied)?;
                let reader = c
                    .services
                    .gitlab_repository_secret_reader()
                    .map_err(denied)?;
                let result = directory
                    .native_push(
                        &granted,
                        reader.as_ref(),
                        prepared_push.ok_or_else(unavailable)?,
                        |sha| {
                            op.primitive(GitReceipt::Push {
                                pushed_sha: sha.into(),
                            });
                        },
                    )
                    .await;
                if let Ok(sha) = result {
                    op.primitive(GitReceipt::Push {
                        pushed_sha: sha.clone(),
                    });
                    op.retain(
                        engine::classify_repository_completion(
                            stamp,
                            RepositoryCompletion::Pushed { sha },
                        )
                        .map_err(denied)?,
                    );
                    c.services
                        .ac_move_stage(&op.query.workspace_id, "committed", "pushed")
                        .await;
                    true
                } else {
                    op.retain(engine::classify_repository_completion(stamp,RepositoryCompletion::Uncertain{message:"HTTPS push completion could not be confirmed for every destination".into()}).map_err(denied)?);
                    false
                }
            }
            Stage::CreatePr => {
                let (request, authority) = stamp.credential_authority().map_err(denied)?;
                let directory = c.services.repository_connection_directory();
                let settled = op.metadata.provider.settled().ok_or_else(unavailable)?;
                let granted = directory
                    .admit(&settled.selected().binding, request, authority)
                    .map_err(denied)?;
                let provider = BoundGitlabRequestCredentials::new(
                    directory,
                    granted,
                    c.services
                        .gitlab_repository_secret_reader()
                        .map_err(denied)?,
                    ACQUIRE,
                )
                .map_err(denied)?
                .into_provider()
                .map_err(denied)?;
                let source = branch_identity(&op.facts.preparation.source)?;
                let target = branch_identity(&op.facts.preparation.target)?;
                let repo = repo_ref(&op.facts.preparation.target.repository)?;
                let input = NewPullRequest {
                    title: command
                        .pr_title
                        .clone()
                        .filter(|s| !s.trim().is_empty())
                        .unwrap_or_else(|| source.branch.clone()),
                    body: command.pr_body.clone(),
                    source_branch: source.branch.clone(),
                    target_branch: target.branch.clone(),
                    draft: false,
                };
                let remote = remote_branch(&provider, &repo, &source.branch).await;
                let local = admission
                    .execution()
                    .map_err(denied)?
                    .git_receipts
                    .iter()
                    .rev()
                    .find_map(|r| match r {
                        GitReceipt::Commit { commit_hash } => Some(commit_hash.clone()),
                        GitReceipt::Push { .. } => None,
                    })
                    .or_else(|| op.facts.preparation.local_head_sha.clone());
                admission
                    .record_publication(publication(op.metadata.root.path(), local, remote))
                    .map_err(denied)?;
                // The complete existing provider consumer checks the project and
                // all matching candidates, then makes its original no-retry POST.
                let result = provider
                    .create_same_project(&repo, input, &source, &target)
                    .await;
                match result {
                    Ok(value) => {
                        let details =
                            actual_review(&op.facts.preparation.target.repository, value.details);
                        op.primitive_review(match value.outcome {
                            ReviewCreateOutcome::Created => Outcome::Created {
                                review: details.clone(),
                            },
                            ReviewCreateOutcome::Reused => Outcome::Reused {
                                review: details.clone(),
                            },
                        });
                        let completion = match value.outcome {
                            ReviewCreateOutcome::Created => RepositoryCompletion::Created(details),
                            ReviewCreateOutcome::Reused => RepositoryCompletion::Reused(details),
                        };
                        op.retain(
                            engine::classify_repository_completion(stamp, completion)
                                .map_err(denied)?,
                        );
                        true
                    }
                    Err(error) => {
                        let uncertain = matches!(error,intent_sourcecontrol::Error::Provider(ref f) if f.kind==intent_sourcecontrol::error::ProviderFailureKind::WriteUncertain);
                        let completion = if uncertain {
                            RepositoryCompletion::Uncertain{message:"GitLab write completion is uncertain; this operation will not retry".into()}
                        } else {
                            RepositoryCompletion::Failed {
                                code: Some("repository-provider-refused".into()),
                                message: "GitLab review could not complete".into(),
                            }
                        };
                        op.retain(
                            engine::classify_repository_completion(stamp, completion)
                                .map_err(denied)?,
                        );
                        false
                    }
                }
            }
        };
        if !result {
            break;
        }
    }
    Ok(())
}
fn create_lock(c: &Connection, op: &Operation) -> Result<Arc<tokio::sync::Mutex<()>>> {
    let selected = op
        .metadata
        .provider
        .settled()
        .ok_or_else(unavailable)?
        .selected();
    let p = &op.facts.preparation;
    let key = serde_json::to_string(&(
        selected.binding.daemon_id.clone(),
        selected.binding.account.instance_base_url.clone(),
        selected.binding.account.account_id.clone(),
        p.target.repository.clone(),
        p.target.provider_project_id.clone(),
        p.source.branch.clone(),
        p.target.branch.clone(),
    ))
    .map_err(denied)?;
    let mut map = c
        .services
        .repository_review_capacity
        .creates
        .lock()
        .map_err(denied)?;
    map.retain(|_, lock| lock.strong_count() != 0);
    if let Some(lock) = map.get(&key).and_then(Weak::upgrade) {
        return Ok(lock);
    }
    let lock = Arc::new(tokio::sync::Mutex::new(()));
    map.insert(key, Arc::downgrade(&lock));
    Ok(lock)
}
fn branch_identity(branch: &intent_core::NativeReviewBranchTarget) -> Result<ReviewBranchIdentity> {
    Ok(ReviewBranchIdentity {
        instance_base_url: branch.repository.instance_base_url.clone(),
        project_id: branch
            .provider_project_id
            .as_ref()
            .ok_or_else(unavailable)?
            .parse()
            .map_err(denied)?,
        project_path: Some(branch.repository.project_path.clone()),
        branch: branch.branch.clone(),
    })
}
fn actual_review(
    target: &RepositoryTarget,
    details: intent_sourcecontrol::ReviewDetails,
) -> Box<intent_core::NativeReviewDetails> {
    use intent_core::{
        NativeReviewBranchIdentity, NativeReviewDetails, NativeReviewState, RepositoryResourceKind,
        ReviewTarget,
    };
    let branch = |b: ReviewBranchIdentity| NativeReviewBranchIdentity {
        provider: RepositoryProvider::Gitlab,
        instance_base_url: b.instance_base_url,
        project_id: b.project_id.to_string(),
        project_path: b.project_path,
        branch: b.branch,
    };
    Box::new(NativeReviewDetails {
        resource: ReviewTarget {
            repository: target.clone(),
            kind: RepositoryResourceKind::MergeRequest,
            number: details.review.number,
        },
        url: details.review.url,
        title: details.review.title,
        body: details.review.body,
        state: details.confirmed_state.map(|s| match s {
            intent_sourcecontrol::ConfirmedReviewState::Open => NativeReviewState::Open,
            intent_sourcecontrol::ConfirmedReviewState::Locked => NativeReviewState::Locked,
            intent_sourcecontrol::ConfirmedReviewState::Closed => NativeReviewState::Closed,
            intent_sourcecontrol::ConfirmedReviewState::Merged => NativeReviewState::Merged,
        }),
        draft: details.confirmed_draft,
        source_branch: details.source.as_ref().map(|b| b.branch.clone()),
        target_branch: details.target.as_ref().map(|b| b.branch.clone()),
        source: details.source.map(branch),
        target: details.target.map(branch),
        author: (!details.review.author.is_empty()).then_some(details.review.author),
        mergeable: details.review.mergeable,
        mergeable_state: details.review.mergeable_state,
        head_sha: details.review.head_sha,
        created_at: Some(details.review.created_at),
        updated_at: Some(details.review.updated_at),
    })
}

#[cfg(test)]
#[path = "native_review/credential_tests.rs"]
mod credential_tests;
#[cfg(test)]
#[path = "native_review/tests.rs"]
mod tests;

struct FinalLock {
    _release: tokio::sync::oneshot::Sender<()>,
}
async fn final_prepare_lock(c: Arc<Connection>, op: Arc<Operation>) -> Result<FinalLock> {
    let capacity = job(&c)?;
    let (life, registration) = c.new_lifetime().map_err(denied)?;
    op.write
        .retirement()
        .native_link(&life.retirement())
        .map_err(denied)?;
    let (tx, rx) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    tokio::spawn(with_caller(
        c.caller.caller().clone(),
        with_wire_credential(c.caller.wire_credential().cloned(), async move {
            let _capacity = capacity;
            let _registration = registration;
            let mut tx = Some(tx);
            let observation_entered = AtomicBool::new(false);
            let retired = life.retirement();
            let result = {
                let work = RepositoryGitSource::with_group(
                    &c.services.store,
                    &c.services.worktree_locks,
                    vec![op.metadata.root.clone()],
                    life.retirement(),
                    |_| async {
                        observation_entered.store(true, Ordering::Release);
                        let owned = op.clone();
                        let matches = tokio::task::spawn_blocking(move || {
                            let (_, actual) = read_context_root_with_resolver(
                                &owned.query.review.root,
                                owned.metadata.root.path(),
                                &saved(&owned.metadata.selection).map_err(local)?,
                                explicit(&owned.query),
                                &resolver(Some(&owned.metadata.provider))?,
                                &GitConfigEnvironment::default(),
                                |target| target_context(target, Some(&owned.metadata.provider)),
                            )
                            .map_err(local)?;
                            if actual != owned.observed
                                || Some(fingerprint(owned.metadata.root.path()).map_err(local)?)
                                    != owned.facts.staging_fingerprint
                                || content_fingerprint(owned.metadata.root.path(), &owned.files)
                                    .map_err(local)?
                                    != owned.content_fingerprint
                            {
                                return Err(AdmissionError::BindingChanged);
                            }
                            Ok(())
                        })
                        .await
                        .map_err(local)?;
                        matches?;
                        op.metadata.validate(true).await.map_err(local)?;
                        if let Some(tx) = tx.take() {
                            if tx.send(Ok(())).is_ok() {
                                let _ = released.await;
                            }
                        }
                        Ok(())
                    },
                );
                tokio::pin!(work);
                tokio::select! {
                    result=&mut work=>result,
                    ()=retired.native_cancelled()=>{ if observation_entered.load(Ordering::Acquire) {work.await} else {Err(AdmissionError::Retired)} },
                    ()=tokio::time::sleep(ACQUIRE)=>{ if observation_entered.load(Ordering::Acquire) {work.await} else {Err(AdmissionError::Retired)} },
                }
            };
            if let Some(tx) = tx.take() {
                let _ = tx.send(result.map_err(denied));
            }
        }),
    ));
    rx.await.map_err(denied)??;
    Ok(FinalLock { _release: release })
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn publication(
    path: &std::path::Path,
    local: Option<String>,
    remote: Result<Option<String>>,
) -> Publication {
    let remote = match remote {
        Ok(None) => {
            return Publication::RemoteBranchMissing {
                local_head_sha: local,
            }
        }
        Ok(Some(remote)) => remote,
        Err(_) => {
            return Publication::Unknown {
                local_head_sha: local,
                remote_source_sha: None,
            }
        }
    };
    let Some(local) = local else {
        return Publication::Unknown {
            local_head_sha: None,
            remote_source_sha: Some(remote),
        };
    };
    let unknown = || Publication::Unknown {
        local_head_sha: Some(local.clone()),
        remote_source_sha: Some(remote.clone()),
    };
    let (Ok(a), Ok(b)) = (git2::Oid::from_str(&local), git2::Oid::from_str(&remote)) else {
        return unknown();
    };
    if a == b {
        return Publication::Included {
            local_head_sha: local,
            remote_source_sha: remote,
        };
    }
    let Ok(repo) = git2::Repository::open(path) else {
        return unknown();
    };
    if repo.find_commit(a).is_err() || repo.find_commit(b).is_err() {
        return unknown();
    }
    match (
        repo.graph_descendant_of(a, b),
        repo.graph_descendant_of(b, a),
    ) {
        (Ok(true), Ok(false)) => Publication::LocalAhead {
            local_head_sha: local,
            remote_source_sha: remote,
        },
        (Ok(false), Ok(true)) => Publication::Included {
            local_head_sha: local,
            remote_source_sha: remote,
        },
        (Ok(false), Ok(false)) => Publication::Diverged {
            local_head_sha: local,
            remote_source_sha: remote,
        },
        _ => unknown(),
    }
}
