//! Original owner-only policy and local-helper frames. No public identifier is
//! authority; prepared private output retains the actual native connection.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use intent_core::caller::{Caller, WireCredential};
use intent_core::gitlab_child_git::{
    GitlabChildGitBinding, GitlabChildGitCredential, GitlabChildGitCredentialQuery,
    GitlabChildGitCredentialResult, GitlabChildGitFrame as Frame, GitlabChildGitPolicy,
    GitlabChildGitPolicyQuery, GitlabChildGitPolicySet, GitlabChildGitUnavailable,
};
use intent_core::repository_request::{
    RepositoryPrivateOutput, RepositoryReadReplyKind, RepositoryReadRequestScope,
};
use intent_core::{BoxFuture, Error, HostRole, Result};
use intent_sourcecontrol::ExposeSecret;
use intent_store::{RepositoryHostAuthoritySnapshot, RepositoryLifecycleKey};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{Duration, Instant};

use super::{Connection, Services};
use crate::repository_admission::lifecycle::{RepositorySourceLifetime, RepositorySubscription};
use crate::repository_admission::AdmissionError;
use crate::source_control_auth_ops::checkout::CheckoutAuthority;
use crate::source_control_auth_ops::repository_owner::{
    GitlabChildCredentialReceipt, GitlabChildOutputGuard, GitlabChildPolicyCapture,
    GitlabChildPolicyExpected, GitlabChildPolicyReceipt,
};

const FRAME_TTL: Duration = Duration::from_secs(15);
const WORK_LIMIT: Duration = Duration::from_secs(10);
fn unavailable() -> Error {
    Error::Forbidden("Child Git unavailable".into())
}
fn denied(_: impl std::fmt::Debug) -> Error {
    unavailable()
}

pub(crate) struct Capacity(pub(super) Arc<Semaphore>);
impl Default for Capacity {
    fn default() -> Self {
        Self(Arc::new(Semaphore::new(64)))
    }
}
pub(super) struct ConnectionState(Arc<Semaphore>);
impl Default for ConnectionState {
    fn default() -> Self {
        Self(Arc::new(Semaphore::new(16)))
    }
}

struct Anchor {
    connection: Arc<Connection>,
    lifetime: RepositorySourceLifetime,
    _registration: RepositorySubscription,
    _subscription: RepositorySubscription,
    _permits: (OwnedSemaphorePermit, OwnedSemaphorePermit),
    authority: Mutex<Option<RepositoryHostAuthoritySnapshot>>,
    authorized: AtomicBool,
    deadline: Instant,
}
impl Anchor {
    fn check(&self) -> Result<()> {
        self.connection.parent.check_current().map_err(denied)?;
        self.lifetime.retirement().check_current().map_err(denied)?;
        if Instant::now() >= self.deadline {
            return Err(unavailable());
        }
        Ok(())
    }
    async fn authorize(&self) -> Result<()> {
        self.check()?;
        let c = &self.connection;
        c.entered().map_err(denied)?;
        c.services
            .require_host_execution("Child Git policy")
            .await
            .map_err(denied)?;
        let Caller::Wire {
            principal_id,
            host_role: HostRole::Owner,
        } = c.caller.caller()
        else {
            return Err(unavailable());
        };
        let hash = match c.caller.wire_credential() {
            Some(WireCredential::Principal { token_hash, .. }) => Some(token_hash.as_str()),
            _ => None,
        };
        let facts = c
            .services
            .store
            .repository_host_authority_snapshot(principal_id, hash)
            .await?;
        if !facts
            .principal
            .value
            .as_ref()
            .is_some_and(|p| &p.id == principal_id && p.is_primary)
            || !facts
                .primary_principal
                .value
                .as_ref()
                .is_some_and(|p| &p.id == principal_id && p.is_primary)
        {
            return Err(unavailable());
        }
        if let Some(WireCredential::Principal {
            principal_id: original,
            ..
        }) = c.caller.wire_credential()
        {
            let credential = facts
                .credential
                .as_ref()
                .and_then(|v| v.value.as_ref())
                .ok_or_else(unavailable)?;
            if original != principal_id
                || &credential.principal_id != original
                || credential.revoked
            {
                return Err(unavailable());
            }
        }
        self.check()?;
        let mut saved = self.authority.lock().map_err(denied)?;
        if saved.as_ref().is_some_and(|old| old != &facts) {
            return Err(unavailable());
        }
        *saved = Some(facts);
        self.authorized.store(true, Ordering::Release);
        Ok(())
    }
    fn end(&self) {
        self.lifetime.retirement().end_scope();
    }
}
impl CheckoutAuthority for Anchor {
    fn dispatch(&self, action: &mut (dyn FnMut() -> Result<()> + Send)) -> Result<()> {
        self.check()?;
        if !self.authorized.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        self.connection
            .parent
            .native_dispatch(|| {
                self.lifetime.retirement().native_dispatch(|| {
                    if Instant::now() >= self.deadline {
                        return Err(AdmissionError::Retired);
                    }
                    Ok(action())
                })
            })
            .map_err(denied)?
    }
}

struct Request {
    weak: Weak<Self>,
    connection: Arc<Connection>,
    frame: Frame,
    anchor: Option<Arc<Anchor>>,
    capture: Mutex<Option<Result<GitlabChildPolicyCapture>>>,
    used: AtomicBool,
    finished: AtomicBool,
    consumed: AtomicBool,
    bound: AtomicBool,
    admitted: Arc<AtomicBool>,
    state: Mutex<State>,
}
#[derive(Clone)]
enum State {
    Empty,
    PublicError,
    Local,
    Credential(Arc<Mutex<Option<GitlabChildCredentialReceipt>>>),
    Policy(Arc<Mutex<Option<GitlabChildPolicyReceipt>>>),
}
tokio::task_local! { static CHILD_REQUEST: Arc<Request>; }

pub(super) fn capture_frame(c: &Connection, frame: Frame) -> Arc<dyn RepositoryReadRequestScope> {
    let captured = (|| {
        c.entered().map_err(denied)?;
        if matches!(frame, Frame::Credential(_)) && !c.caller.is_admitted_local() {
            return Err(unavailable());
        }
        let local = c.child_git.0.clone().try_acquire_owned().map_err(denied)?;
        let global = c
            .services
            .repository_child_git_capacity
            .0
            .clone()
            .try_acquire_owned()
            .map_err(denied)?;
        let (lifetime, registration) = c.new_lifetime().map_err(denied)?;
        let subscription = lifetime
            .subscribe(
                &c.services.store,
                c.caller.caller(),
                &[
                    RepositoryLifecycleKey::Database,
                    RepositoryLifecycleKey::WireAuthority,
                ],
            )
            .map_err(denied)?;
        let anchor = Arc::new(Anchor {
            connection: c.weak.upgrade().ok_or_else(unavailable)?,
            lifetime,
            _registration: registration,
            _subscription: subscription,
            _permits: (local, global),
            authority: Mutex::new(None),
            authorized: AtomicBool::new(false),
            deadline: Instant::now() + FRAME_TTL,
        });
        // This is intentionally before queue admission and the first permission
        // await. No later account may replace this original captured producer.
        let provider = c.services.capture_gitlab_child_policy();
        Ok::<_, Error>((anchor, provider))
    })();
    let (anchor, capture) = match captured {
        Ok((a, p)) => (Some(a), Some(p)),
        Err(_) => (None, None),
    };
    Arc::new_cyclic(|weak| Request {
        weak: weak.clone(),
        connection: c.weak.upgrade().expect("owned original connection"),
        frame,
        anchor,
        capture: Mutex::new(capture),
        used: AtomicBool::new(false),
        finished: AtomicBool::new(false),
        consumed: AtomicBool::new(false),
        bound: AtomicBool::new(false),
        admitted: Arc::new(AtomicBool::new(false)),
        state: Mutex::new(State::Empty),
    })
}
impl Request {
    fn current(services: &Services, frame: &Frame) -> Result<Arc<Self>> {
        let r = CHILD_REQUEST.try_with(Clone::clone).map_err(denied)?;
        if !std::ptr::eq(r.connection.services.as_ref(), services)
            || &r.frame != frame
            || r.used.swap(true, Ordering::AcqRel)
        {
            return Err(unavailable());
        }
        r.check()?;
        Ok(r)
    }
    fn check(&self) -> Result<()> {
        if self.finished.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        self.anchor.as_ref().ok_or_else(unavailable)?.check()
    }
    fn finish(&self) {
        if self.finished.swap(true, Ordering::AcqRel) {
            return;
        }
        if !self.admitted.load(Ordering::Acquire) {
            if let Some(anchor) = &self.anchor {
                anchor.end();
            }
        }
    }
    fn state(&self, state: State) -> Result<()> {
        *self.state.lock().map_err(denied)? = state;
        Ok(())
    }
}
impl Drop for Request {
    fn drop(&mut self) {
        self.finish();
    }
}
impl RepositoryReadRequestScope for Request {
    fn scope<'a>(&'a self, body: BoxFuture<'a, ()>) -> BoxFuture<'a, ()> {
        Box::pin(CHILD_REQUEST.scope(self.weak.upgrade().expect("owned original frame"), body))
    }
    fn retire(&self) {
        self.finish();
    }
    fn bind_private_output(&self, frame: &str) -> Result<Option<Arc<dyn RepositoryPrivateOutput>>> {
        self.check()?;
        if self.bound.swap(true, Ordering::AcqRel) {
            return Err(unavailable());
        }
        match self.state.lock().map_err(denied)?.clone() {
            State::PublicError => Ok(None),
            State::Empty => Err(unavailable()),
            state => {
                let guard = match state {
                    State::Credential(receipt) => Some(
                        receipt
                            .lock()
                            .map_err(denied)?
                            .take()
                            .ok_or_else(unavailable)?
                            .into_output()?,
                    ),
                    State::Policy(receipt) => Some(
                        receipt
                            .lock()
                            .map_err(denied)?
                            .take()
                            .ok_or_else(unavailable)?
                            .into_output()?,
                    ),
                    State::Local => None,
                    State::Empty | State::PublicError => return Err(unavailable()),
                };
                Ok(Some(Arc::new(Output {
                    frame: frame.into(),
                    anchor: self.anchor.clone().ok_or_else(unavailable)?,
                    guard,
                    admitted: self.admitted.clone(),
                })))
            }
        }
    }
    fn deliver<'a>(
        &'a self,
        kind: RepositoryReadReplyKind,
        transfer: &'a mut (dyn FnMut() -> Result<()> + Send),
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let state = self.state.lock().map_err(denied)?.clone();
            if matches!(state, State::PublicError) {
                if kind != RepositoryReadReplyKind::ServiceError
                    || self.consumed.swap(true, Ordering::AcqRel)
                {
                    return Err(unavailable());
                }
                return transfer();
            }
            if matches!(state, State::Empty) || kind != RepositoryReadReplyKind::Result {
                return Err(unavailable());
            }
            self.check()?;
            let anchor = self.anchor.as_ref().ok_or_else(unavailable)?;
            tokio::time::timeout(WORK_LIMIT, async {
                let _legacy = self
                    .connection
                    .caller
                    .legacy_lease()
                    .await
                    .map_err(denied)?;
                anchor.authorize().await?;
                anchor.dispatch(&mut || {
                    if self.consumed.swap(true, Ordering::AcqRel) {
                        return Err(unavailable());
                    }
                    self.admitted.store(true, Ordering::Release);
                    let result = transfer();
                    if result.is_err() {
                        self.admitted.store(false, Ordering::Release);
                    }
                    result
                })
            })
            .await
            .map_err(denied)?
        })
    }
}
struct Output {
    frame: String,
    anchor: Arc<Anchor>,
    guard: Option<GitlabChildOutputGuard>,
    admitted: Arc<AtomicBool>,
}
impl RepositoryPrivateOutput for Output {
    fn frame(&self) -> &str {
        &self.frame
    }
    fn with_current(&self, transfer: &mut (dyn FnMut() -> Result<()> + Send)) -> Result<()> {
        if !self.admitted.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        // The credential receipt calls this SAME original R authority before
        // its P proof. Do not nest Anchor.dispatch around it a second time.
        match &self.guard {
            Some(guard) => guard.with_current(transfer),
            None => self.anchor.dispatch(transfer),
        }
    }
}

pub(crate) fn credential(
    services: &Services,
    q: GitlabChildGitCredentialQuery,
) -> BoxFuture<'_, Result<GitlabChildGitCredentialResult>> {
    let captured = Request::current(services, &Frame::Credential(q.clone()));
    Box::pin(async move {
        let r = captured?;
        let result = tokio::time::timeout(WORK_LIMIT, async {
            if !r.connection.caller.is_admitted_local() {
                return Err(unavailable());
            }
            let _legacy = r.connection.caller.legacy_lease().await.map_err(denied)?;
            let anchor = r.anchor.clone().ok_or_else(unavailable)?;
            anchor.authorize().await?;
            let capture = r
                .capture
                .lock()
                .map_err(denied)?
                .take()
                .ok_or_else(unavailable)?;
            let Ok(capture) = capture else {
                r.state(State::Local)?;
                return Ok(GitlabChildGitCredentialResult { credential: None });
            };
            let value = capture
                .credential(&q.protocol, &q.host, &q.path, q.username.as_deref(), anchor)
                .await?;
            let credential = if let Some(value) = value {
                let (token, receipt) = value.into_parts();
                r.state(State::Credential(Arc::new(Mutex::new(Some(receipt)))))?;
                Some(GitlabChildGitCredential {
                    username: "oauth2".into(),
                    password: token.expose_secret().to_owned(),
                })
            } else {
                r.state(State::Local)?;
                None
            };
            Ok(GitlabChildGitCredentialResult { credential })
        })
        .await
        .map_err(denied)
        .and_then(std::convert::identity);
        if result.is_err() {
            r.state(State::PublicError)?;
        }
        result
    })
}

pub(crate) fn policy_get(
    services: &Services,
    query: GitlabChildGitPolicyQuery,
) -> BoxFuture<'_, Result<GitlabChildGitPolicy>> {
    let captured = Request::current(services, &Frame::Get(query));
    Box::pin(policy_reply(captured, None))
}

pub(crate) fn policy_set(
    services: &Services,
    query: GitlabChildGitPolicySet,
) -> BoxFuture<'_, Result<GitlabChildGitPolicy>> {
    let captured = Request::current(services, &Frame::Set(query.clone()));
    Box::pin(policy_reply(captured, Some(query)))
}

async fn policy_reply(
    captured: Result<Arc<Request>>,
    update: Option<GitlabChildGitPolicySet>,
) -> Result<GitlabChildGitPolicy> {
    let r = captured?;
    let result = tokio::time::timeout(WORK_LIMIT, async {
        let _legacy = r.connection.caller.legacy_lease().await.map_err(denied)?;
        let anchor = r.anchor.clone().ok_or_else(unavailable)?;
        anchor.authorize().await?;
        let capture = r
            .capture
            .lock()
            .map_err(denied)?
            .take()
            .ok_or_else(unavailable)?;
        let capture = match capture {
            Ok(capture) => capture,
            Err(_) if update.is_none() => {
                r.state(State::Local)?;
                return Ok(GitlabChildGitPolicy::Unavailable {
                    reason: GitlabChildGitUnavailable::Unavailable,
                });
            }
            Err(_) => return Err(unavailable()),
        };
        let output = if let Some(query) = update {
            let binding = query.binding;
            let revision: u64 = binding.revision.parse().map_err(denied)?;
            if revision.to_string() != binding.revision {
                return Err(unavailable());
            }
            let expected = GitlabChildPolicyExpected {
                instance_base_url: binding.instance_base_url,
                account_id: binding.connection.account_id,
                connection_id: binding.connection.connection_id,
                connection_generation: binding.connection.connection_generation,
                revision,
            };
            capture.set(expected, query.enabled, anchor).await?
        } else {
            capture.status(anchor).await?
        };
        let (status, receipt) = output.into_parts();
        r.state(State::Policy(Arc::new(Mutex::new(Some(receipt)))))?;
        Ok(GitlabChildGitPolicy::Connected {
            binding: GitlabChildGitBinding {
                instance_base_url: status.instance_base_url,
                connection: intent_core::RepositoryConnectionScope {
                    connection_id: status.connection_id,
                    account_id: status.account_id,
                    connection_generation: status.connection_generation,
                },
                revision: status.revision.to_string(),
            },
            enabled: status.enabled,
            effective: status.enabled,
        })
    })
    .await
    .map_err(denied)
    .and_then(std::convert::identity);
    if result.is_err() {
        r.state(State::PublicError)?;
    }
    result
}
