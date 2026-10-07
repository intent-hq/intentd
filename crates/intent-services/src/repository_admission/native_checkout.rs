//! Host-admitted browsing before a workspace exists. Every request belongs to
//! the original native socket and original checkout/credential lifetime.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use base64::Engine;
use intent_core::caller::{with_caller, with_wire_credential, Caller, WireCredential};
use intent_core::repository_checkout::{
    CheckoutBinding, CheckoutBranch, CheckoutBranches, CheckoutBranchesQuery, CheckoutCapture,
    CheckoutCaptureQuery, CheckoutFrame, CheckoutMode, CheckoutProject, CheckoutProjectDetail,
    CheckoutProjectQuery, CheckoutProjects, CheckoutProjectsQuery, CheckoutReleased,
    CheckoutResult, CheckoutSelection, CheckoutUnavailable, CheckoutWarm,
};
use intent_core::repository_request::{RepositoryReadReplyKind, RepositoryReadRequestScope};
use intent_core::{BoxFuture, Error, HostRole, Result};
use intent_sourcecontrol::{GitlabInstance, PageParams, SourceControl};
use intent_store::{RepositoryHostAuthoritySnapshot, RepositoryLifecycleKey};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use super::{Connection, Services};
use crate::repository_admission::lifecycle::{RepositorySourceLifetime, RepositorySubscription};
use crate::repository_admission::AdmissionError;
use crate::settings_registry::{SettingsRegistry, SettingsSnapshot};
use crate::source_control_auth_ops::checkout::{
    CheckoutAuthority, GitlabCheckoutCapture, GitlabCheckoutConnection,
};

const TTL: Duration = Duration::from_secs(600);
const READ_LIMIT: Duration = Duration::from_secs(15);
const CAPTURE_LIMIT: Duration = Duration::from_secs(5);
const MAX_RECORDS: usize = 1024;

fn unavailable() -> Error {
    Error::Forbidden("Repository checkout unavailable".into())
}
fn denied(_: impl std::fmt::Debug) -> Error {
    unavailable()
}
fn keys() -> [RepositoryLifecycleKey; 2] {
    [
        RepositoryLifecycleKey::Database,
        RepositoryLifecycleKey::WireAuthority,
    ]
}

pub(crate) struct Capacity {
    leases: Arc<Semaphore>,
    frames: Arc<Semaphore>,
}
impl Default for Capacity {
    fn default() -> Self {
        Self {
            leases: Arc::new(Semaphore::new(64)),
            frames: Arc::new(Semaphore::new(128)),
        }
    }
}
pub(super) struct ConnectionState {
    closed: AtomicBool,
    leases: Mutex<HashMap<String, Arc<Lease>>>,
    frames: Arc<Semaphore>,
}
impl Default for ConnectionState {
    fn default() -> Self {
        Self {
            closed: AtomicBool::new(false),
            leases: Mutex::default(),
            frames: Arc::new(Semaphore::new(16)),
        }
    }
}
impl ConnectionState {
    pub(super) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        let leases = self
            .leases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain()
            .map(|(_, lease)| lease)
            .collect::<Vec<_>>();
        for lease in leases {
            lease.lifetime.retirement().end_scope();
        }
    }
    fn retire(&self, id: &str) {
        let lease = self
            .leases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(id);
        if let Some(lease) = lease {
            lease.lifetime.retirement().end_scope();
        }
    }
}

#[derive(Clone)]
struct Project {
    wire: CheckoutProject,
}
#[derive(Clone, PartialEq, Eq)]
struct PageScope {
    project: Option<String>,
    query: String,
    limit: u8,
    cached: bool,
}
#[derive(Clone)]
struct Cursor {
    scope: PageScope,
    provider: String,
}
struct Lease {
    id: String,
    revision: String,
    connection: Weak<Connection>,
    authority: RepositoryHostAuthoritySnapshot,
    lifetime: RepositorySourceLifetime,
    _registration: RepositorySubscription,
    _subscription: RepositorySubscription,
    _permit: OwnedSemaphorePermit,
    provider: OnceLock<Arc<GitlabCheckoutConnection>>,
    include_owner_avatar: bool,
    projects: Mutex<HashMap<String, Project>>,
    branches: Mutex<HashMap<(String, String), CheckoutBranch>>,
    cursors: Mutex<HashMap<String, Cursor>>,
    deadline: Instant,
    published: AtomicBool,
    workspace: Option<intent_store::RepositoryWorkspaceAuthoritySnapshot>,
}

struct Request {
    weak: Weak<Self>,
    connection: Arc<Connection>,
    frame: CheckoutFrame,
    lifetime: Option<RepositorySourceLifetime>,
    _registration: Option<RepositorySubscription>,
    _subscription: Option<RepositorySubscription>,
    _permits: Option<(OwnedSemaphorePermit, OwnedSemaphorePermit)>,
    used: AtomicBool,
    finished: AtomicBool,
    consumed: AtomicBool,
    output: Mutex<Output>,
    deadline: Instant,
}
#[derive(Clone)]
enum Output {
    Empty,
    Public,
    Legacy,
    Private {
        lease: Arc<Lease>,
        provider: Arc<GitlabCheckoutConnection>,
        projects: Vec<String>,
    },
}
tokio::task_local! { static CHECKOUT_REQUEST: Arc<Request>; }

pub(super) fn capture_frame(
    connection: &Connection,
    frame: CheckoutFrame,
) -> Arc<dyn RepositoryReadRequestScope> {
    let captured = (|| {
        connection.entered().map_err(denied)?;
        if connection.checkout.closed.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        let local = connection
            .checkout
            .frames
            .clone()
            .try_acquire_owned()
            .map_err(denied)?;
        let global = connection
            .services
            .repository_checkout_capacity
            .frames
            .clone()
            .try_acquire_owned()
            .map_err(denied)?;
        let (lifetime, registration) = connection.new_lifetime().map_err(denied)?;
        let subscription = lifetime
            .subscribe(
                &connection.services.store,
                connection.caller.caller(),
                &keys(),
            )
            .map_err(denied)?;
        Ok::<_, Error>((lifetime, registration, subscription, local, global))
    })();
    let (lifetime, registration, subscription, permits) = match captured {
        Ok((lifetime, registration, subscription, local, global)) => (
            Some(lifetime),
            Some(registration),
            Some(subscription),
            Some((local, global)),
        ),
        Err(_) => (None, None, None, None),
    };
    Arc::new_cyclic(|weak| Request {
        weak: weak.clone(),
        connection: connection
            .weak
            .upgrade()
            .expect("original connection owned by transport"),
        frame,
        lifetime,
        _registration: registration,
        _subscription: subscription,
        _permits: permits,
        used: AtomicBool::new(false),
        finished: AtomicBool::new(false),
        consumed: AtomicBool::new(false),
        output: Mutex::new(Output::Empty),
        deadline: Instant::now() + TTL,
    })
}

impl Request {
    fn current(services: &Services, frame: &CheckoutFrame) -> Result<Arc<Self>> {
        let request = CHECKOUT_REQUEST.try_with(Clone::clone).map_err(denied)?;
        if !std::ptr::eq(request.connection.services.as_ref(), services)
            || request.frame != *frame
            || request.used.swap(true, Ordering::AcqRel)
        {
            return Err(unavailable());
        }
        request.connection.entered().map_err(denied)?;
        request.check()?;
        Ok(request)
    }
    fn check(&self) -> Result<()> {
        if self.finished.load(Ordering::Acquire)
            || Instant::now() >= self.deadline
            || self.connection.checkout.closed.load(Ordering::Acquire)
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
    async fn authority(&self) -> Result<RepositoryHostAuthoritySnapshot> {
        self.check()?;
        self.connection.entered().map_err(denied)?;
        let Caller::Wire {
            principal_id,
            host_role,
        } = self.connection.caller.caller()
        else {
            return Err(unavailable());
        };
        let hash = match self.connection.caller.wire_credential() {
            Some(WireCredential::Principal { token_hash, .. }) => Some(token_hash.as_str()),
            _ => None,
        };
        let facts = self
            .connection
            .services
            .store
            .repository_host_authority_snapshot(principal_id, hash)
            .await?;
        let person = facts
            .principal
            .value
            .as_ref()
            .filter(|p| &p.id == principal_id)
            .ok_or_else(unavailable)?;
        let primary = facts
            .primary_principal
            .value
            .as_ref()
            .filter(|p| p.is_primary);
        let owner = person.is_primary && primary.is_some_and(|p| &p.id == principal_id);
        if !owner && facts.host_member.value.is_none() {
            return Err(unavailable());
        }
        if *host_role == HostRole::Owner && !owner {
            return Err(unavailable());
        }
        match self.connection.caller.wire_credential() {
            Some(WireCredential::Principal {
                principal_id: original,
                ..
            }) => {
                let credential = facts
                    .credential
                    .as_ref()
                    .and_then(|c| c.value.as_ref())
                    .ok_or_else(unavailable)?;
                if credential.revoked
                    || &credential.principal_id != original
                    || original != principal_id
                {
                    return Err(unavailable());
                }
            }
            Some(WireCredential::Legacy { .. }) | None if owner => {}
            _ => return Err(unavailable()),
        }
        self.check()?;
        Ok(facts)
    }
    async fn bind(
        self: &Arc<Self>,
        id: &str,
        revision: &str,
    ) -> Result<(Arc<Lease>, Arc<GitlabCheckoutConnection>)> {
        let lease = self
            .connection
            .checkout
            .leases
            .lock()
            .map_err(denied)?
            .get(id)
            .cloned()
            .ok_or_else(unavailable)?;
        if revision != lease.revision || !lease.published.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        self.validate(&lease).await?;
        let provider = lease
            .provider
            .get()
            .ok_or_else(unavailable)?
            .for_request(Arc::new(RequestAuthority {
                request: Arc::downgrade(self),
                lease: lease.clone(),
            }));
        provider.with_current(&mut || Ok(()))?;
        *self.output.lock().map_err(denied)? = Output::Private {
            lease: lease.clone(),
            provider: provider.clone(),
            projects: vec![],
        };
        Ok((lease, provider))
    }
    async fn validate(&self, lease: &Lease) -> Result<()> {
        if let Some(original) = &lease.workspace {
            if self
                .connection
                .services
                .store
                .repository_workspace_authority_snapshot(&original.workspace_id)
                .await?
                != *original
            {
                return Err(unavailable());
            }
        }
        if self.authority().await? != lease.authority {
            return Err(unavailable());
        }
        lease.check()
    }
    fn private_projects(&self, projects: Vec<String>) -> Result<()> {
        let mut output = self.output.lock().map_err(denied)?;
        let Output::Private {
            projects: stored, ..
        } = &mut *output
        else {
            return Err(unavailable());
        };
        *stored = projects;
        Ok(())
    }
    fn public(&self) -> Result<()> {
        *self.output.lock().map_err(denied)? = Output::Public;
        Ok(())
    }
    fn finish(&self) {
        if self.finished.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(lifetime) = &self.lifetime {
            lifetime.retirement().end_scope();
        }
        let output = std::mem::replace(
            &mut *self
                .output
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            Output::Empty,
        );
        if let Output::Private { lease, .. } = output {
            if !lease.published.load(Ordering::Acquire) {
                self.connection.checkout.retire(&lease.id);
            }
        }
    }
}

impl Lease {
    fn check(&self) -> Result<()> {
        if Instant::now() >= self.deadline {
            return Err(unavailable());
        }
        self.lifetime.retirement().check_current().map_err(denied)
    }
    fn dispatch(
        &self,
        request: Option<&Request>,
        action: &mut (dyn FnMut() -> Result<()> + Send),
    ) -> Result<()> {
        let c = self.connection.upgrade().ok_or_else(unavailable)?;
        self.check()?;
        if let Some(r) = request {
            r.check()?;
        }
        c.parent
            .native_dispatch(|| {
                self.lifetime.retirement().native_dispatch(|| {
                    let mut consume = || {
                        if Instant::now() >= self.deadline {
                            return Err(AdmissionError::Retired);
                        }
                        // The original provider adapter holds its GitLab config,
                        // credential generation and denial guards through this
                        // action. Unrelated settings publication is not a new
                        // repository authority or a reason to retire this lease.
                        Ok(action())
                    };
                    match request {
                        Some(r) => r
                            .lifetime
                            .as_ref()
                            .ok_or(AdmissionError::Retired)?
                            .retirement()
                            .native_dispatch(|| {
                                if r.finished.load(Ordering::Acquire)
                                    || Instant::now() >= r.deadline
                                {
                                    return Err(AdmissionError::Retired);
                                }
                                consume()
                            }),
                        None => consume(),
                    }
                })
            })
            .map_err(denied)?
    }
}
struct LeaseAuthority(Weak<Lease>);
impl CheckoutAuthority for LeaseAuthority {
    fn dispatch(&self, action: &mut (dyn FnMut() -> Result<()> + Send)) -> Result<()> {
        self.0
            .upgrade()
            .ok_or_else(unavailable)?
            .dispatch(None, action)
    }
}
struct RequestAuthority {
    request: Weak<Request>,
    lease: Arc<Lease>,
}
impl CheckoutAuthority for RequestAuthority {
    fn dispatch(&self, action: &mut (dyn FnMut() -> Result<()> + Send)) -> Result<()> {
        let request = self.request.upgrade().ok_or_else(unavailable)?;
        self.lease.dispatch(Some(&request), action)
    }
}

impl RepositoryReadRequestScope for Request {
    fn scope<'a>(&'a self, body: BoxFuture<'a, ()>) -> BoxFuture<'a, ()> {
        Box::pin(CHECKOUT_REQUEST.scope(
            self.weak.upgrade().expect("owned original checkout frame"),
            with_caller(
                self.connection.caller.caller().clone(),
                with_wire_credential(self.connection.caller.wire_credential().cloned(), body),
            ),
        ))
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
            tokio::time::timeout(CAPTURE_LIMIT, async {
                self.check()?;
                let output = self.output.lock().map_err(denied)?.clone();
                let _legacy = self
                    .connection
                    .caller
                    .legacy_lease()
                    .await
                    .map_err(denied)?;
                if matches!(output, Output::Legacy) {
                    return self
                        .connection
                        .parent
                        .native_dispatch(|| {
                            if self.consumed.swap(true, Ordering::AcqRel) {
                                return Err(AdmissionError::Retired);
                            }
                            Ok(transfer())
                        })
                        .map_err(denied)?;
                }
                let current = self.authority().await?;
                match output {
                    Output::Empty | Output::Legacy => Err(unavailable()),
                    Output::Public => self
                        .connection
                        .parent
                        .native_dispatch(|| {
                            self.lifetime
                                .as_ref()
                                .ok_or(AdmissionError::Retired)?
                                .retirement()
                                .native_dispatch(|| {
                                    if self.consumed.swap(true, Ordering::AcqRel) {
                                        return Err(AdmissionError::Retired);
                                    }
                                    Ok(transfer())
                                })
                        })
                        .map_err(denied)?,
                    Output::Private {
                        lease,
                        provider,
                        projects,
                    } => {
                        self.validate(&lease).await?;
                        if kind != RepositoryReadReplyKind::Result || current != lease.authority {
                            return Err(unavailable());
                        }
                        provider.with_projects_current(&projects, &mut || {
                            if self.consumed.swap(true, Ordering::AcqRel) {
                                return Err(unavailable());
                            }
                            transfer()?;
                            lease.published.store(true, Ordering::Release);
                            Ok(())
                        })
                    }
                }
            })
            .await
            .map_err(denied)?
        })
    }
}

fn entry<'a, T: Send + 'a>(
    services: &'a Services,
    frame: &CheckoutFrame,
    body: impl FnOnce(Arc<Request>) -> BoxFuture<'a, Result<T>> + Send + 'a,
) -> BoxFuture<'a, Result<T>> {
    let request = Request::current(services, frame);
    Box::pin(async move {
        let request = request?;
        let _legacy = tokio::time::timeout(CAPTURE_LIMIT, request.connection.caller.legacy_lease())
            .await
            .map_err(denied)?
            .map_err(denied)?;
        let result = body(request.clone()).await;
        if result.is_err() {
            request.public()?;
        }
        result
    })
}

struct OriginalConnection {
    registry: Arc<SettingsRegistry>,
    settings: Arc<SettingsSnapshot>,
    provider: Result<GitlabCheckoutCapture>,
}

fn capture_connection(services: &Services) -> Option<OriginalConnection> {
    services.settings_registry.clone().map(|registry| {
        let settings = registry.snapshot();
        let provider = services.capture_gitlab_checkout_connection();
        OriginalConnection {
            registry,
            settings,
            provider,
        }
    })
}

async fn admit_lease(
    r: &Arc<Request>,
    original: OriginalConnection,
    expected: Option<&str>,
    workspace: Option<intent_store::RepositoryWorkspaceAuthoritySnapshot>,
    extra_keys: &[RepositoryLifecycleKey],
) -> Result<std::result::Result<(Arc<Lease>, Arc<GitlabCheckoutConnection>), CheckoutUnavailable>> {
    let authority = tokio::time::timeout(CAPTURE_LIMIT, r.authority())
        .await
        .map_err(denied)??;
    let OriginalConnection {
        registry: _,
        settings,
        provider,
    } = original;
    let configured = &settings.effective.source_control.gitlab;
    let instance = crate::source_control_auth_ops::repository_owner::logical_instance(configured)?;
    if expected
        .is_some_and(|expected| GitlabInstance::parse(expected).ok().as_ref() != Some(&instance))
    {
        return Ok(Err(CheckoutUnavailable::InvalidTarget));
    }
    let permit = r
        .connection
        .services
        .repository_checkout_capacity
        .leases
        .clone()
        .try_acquire_owned()
        .map_err(denied)?;

    let (lifetime, registration) = r.connection.new_lifetime().map_err(denied)?;
    let mut coordinates = keys().to_vec();
    coordinates.extend_from_slice(extra_keys);
    let subscription = lifetime
        .subscribe(
            &r.connection.services.store,
            r.connection.caller.caller(),
            &coordinates,
        )
        .map_err(denied)?;
    if r.authority().await? != authority {
        return Err(unavailable());
    }
    let lease = Arc::new(Lease {
        id: uuid::Uuid::new_v4().to_string(),
        revision: uuid::Uuid::new_v4().to_string(),
        connection: r.connection.weak.clone(),
        authority,
        lifetime,
        _registration: registration,
        _subscription: subscription,
        _permit: permit,
        provider: OnceLock::new(),
        include_owner_avatar: matches!(
            &r.frame,
            CheckoutFrame::Capture(q) if q.include_owner_avatar == Some(true)
        ),
        projects: Mutex::default(),
        branches: Mutex::default(),
        cursors: Mutex::default(),
        deadline: Instant::now() + TTL,
        published: AtomicBool::new(false),
        workspace,
    });
    let Ok(captured) = provider else {
        return Ok(Err(CheckoutUnavailable::NotConnected));
    };
    let Ok(provider) = captured.admit(Arc::new(LeaseAuthority(Arc::downgrade(&lease)))) else {
        return Ok(Err(CheckoutUnavailable::Retired));
    };
    if provider.instance_base_url() != instance.as_str() {
        return Err(unavailable());
    }
    lease
        .provider
        .set(provider.clone())
        .map_err(|_| unavailable())?;
    let original = provider.for_request(Arc::new(RequestAuthority {
        request: Arc::downgrade(r),
        lease: lease.clone(),
    }));
    original.with_current(&mut || Ok(()))?;
    Ok(Ok((lease, original)))
}

pub(crate) fn capture(
    services: &Services,
    q: CheckoutCaptureQuery,
) -> BoxFuture<'_, Result<CheckoutResult<CheckoutCapture>>> {
    let original = capture_connection(services);
    entry(services, &CheckoutFrame::Capture(q.clone()), move |r| {
        Box::pin(async move {
            if q.provider != "gitlab" {
                return Err(Error::InvalidParams(
                    "checkout provider must be gitlab".into(),
                ));
            }
            let (lease, original) = match admit_lease(
                &r,
                original.ok_or_else(unavailable)?,
                q.instance_base_url.as_deref(),
                None,
                &[],
            )
            .await?
            {
                Ok(value) => value,
                Err(reason) => {
                    r.public()?;
                    return Ok(CheckoutResult::unavailable(reason));
                }
            };
            {
                let mut leases = r.connection.checkout.leases.lock().map_err(denied)?;
                if leases.len() >= 16 {
                    return Err(unavailable());
                }
                leases.insert(lease.id.clone(), lease.clone());
            }
            *r.output.lock().map_err(denied)? = Output::Private {
                lease: lease.clone(),
                provider: original.clone(),
                projects: vec![],
            };
            let c = r.connection.weak.clone();
            let id = lease.id.clone();
            let stop = lease.lifetime.retirement();
            let deadline = lease.deadline;
            // caller-binding: allow — retires only the original checkout lease, no service capability calls
            tokio::spawn(async move {
                tokio::select! { ()=stop.native_cancelled()=>{}, ()=tokio::time::sleep_until(deadline)=>{} }
                if let Some(c) = c.upgrade() {
                    c.checkout.retire(&id);
                }
            });
            Ok(CheckoutResult::Ready {
                value: CheckoutCapture {
                    checkout_id: lease.id.clone(),
                    revision: lease.revision.clone(),
                    provider: "gitlab".into(),
                    instance_base_url: original.instance_base_url().into(),
                    expires_after_ms: 600_000,
                },
            })
        })
    })
}

impl Drop for Request {
    fn drop(&mut self) {
        self.finish();
    }
}

fn page_scope(
    project: Option<String>,
    query: Option<String>,
    limit: Option<u32>,
    cached: bool,
) -> Result<PageScope> {
    let query = query.unwrap_or_default().trim().to_string();
    if query.len() > 256 || query.chars().any(char::is_control) {
        return Err(Error::InvalidParams("invalid checkout search".into()));
    }
    let limit = limit.unwrap_or(50);
    if !(1..=100).contains(&limit) {
        return Err(Error::InvalidParams(
            "checkout limit must be between 1 and 100".into(),
        ));
    }
    Ok(PageScope {
        project,
        query,
        limit: u8::try_from(limit).map_err(denied)?,
        cached,
    })
}
impl Lease {
    fn page(&self, scope: &PageScope, cursor: Option<&str>) -> Result<PageParams> {
        let provider = match cursor {
            None => None,
            Some(id) => {
                let map = self.cursors.lock().map_err(denied)?;
                let value = map.get(id).filter(|c| c.scope == *scope).ok_or_else(|| {
                    Error::InvalidParams(
                        "checkout cursor belongs to another connection, project or search".into(),
                    )
                })?;
                Some(value.provider.clone())
            }
        };
        Ok(PageParams {
            limit: scope.limit,
            cursor: provider,
        })
    }
    fn next_cursor(&self, scope: PageScope, provider: Option<String>) -> Result<Option<String>> {
        let Some(provider) = provider else {
            return Ok(None);
        };
        let mut map = self.cursors.lock().map_err(denied)?;
        if map.len() >= MAX_RECORDS {
            // Bound retained history, not project/branch reachability. The new
            // cursor remains usable; an evicted old cursor requires a restart.
            map.clear();
        }
        let id = uuid::Uuid::new_v4().to_string();
        map.insert(id.clone(), Cursor { scope, provider });
        Ok(Some(id))
    }
    fn remember(&self, project: Project) -> Result<()> {
        let mut projects = self.projects.lock().map_err(denied)?;
        if projects.len() >= MAX_RECORDS && !projects.contains_key(&project.wire.project_path) {
            projects.clear();
        }
        projects.insert(project.wire.project_path.clone(), project);
        Ok(())
    }
}
fn split_project(path: &str) -> Result<(&str, &str)> {
    if path.len() > 1024
        || !path.is_ascii()
        || path.split('/').any(|s| {
            s.is_empty()
                || s == "."
                || s == ".."
                || !s
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        })
    {
        return Err(Error::InvalidParams("invalid GitLab project path".into()));
    }
    path.rsplit_once('/')
        .ok_or_else(|| Error::InvalidParams("GitLab project needs a namespace".into()))
}
fn project_wire(
    instance: &str,
    repo: intent_sourcecontrol::Repo,
    include_owner_avatar: bool,
) -> Result<Project> {
    let project_path = format!("{}/{}", repo.owner, repo.name);
    split_project(&project_path)?;
    let web_url = format!("{}/{project_path}", instance.trim_end_matches('/'));
    if repo
        .url
        .as_deref()
        .is_some_and(|url| url.trim_end_matches('/') != web_url)
    {
        return Err(unavailable());
    }
    let default_branch = repo.default_branch.filter(|s| !s.trim().is_empty());
    Ok(Project {
        wire: CheckoutProject {
            project_path,
            name: repo.name,
            namespace: repo.owner,
            clone_url: format!("{web_url}.git"),
            web_url,
            default_branch,
            owner_avatar_url: repo.owner_avatar_url.filter(|_| include_owner_avatar),
        },
    })
}
fn project_from_url(instance: &str, raw: &str) -> Result<String> {
    if raw.len() > 8192 || raw.chars().any(char::is_control) {
        return Err(Error::InvalidParams("invalid GitLab URL".into()));
    }
    let instance = GitlabInstance::parse(instance).map_err(denied)?;
    // Context query/fragment are preserved for the caller but do not change
    // the repository location. Validate the original, unnormalised location.
    let location = raw.split(['?', '#']).next().unwrap_or(raw);
    if !instance.contains_url(location) {
        return Err(Error::InvalidParams(
            "URL is outside the selected GitLab instance".into(),
        ));
    }
    let root = reqwest::Url::parse(instance.as_str()).map_err(denied)?;
    let url = reqwest::Url::parse(raw).map_err(denied)?;
    let relative = url
        .path()
        .strip_prefix(root.path().trim_end_matches('/'))
        .and_then(|p| p.strip_prefix('/'))
        .ok_or_else(unavailable)?;
    let project = if let Some((path, resource)) = relative.split_once("/-/") {
        let parts = resource
            .trim_end_matches('/')
            .split('/')
            .collect::<Vec<_>>();
        if parts.len() != 2
            || !matches!(parts[0], "merge_requests" | "issues")
            || parts[1].parse::<u64>().ok().is_none_or(|n| n == 0)
        {
            return Err(Error::InvalidParams(
                "unsupported GitLab resource URL".into(),
            ));
        }
        path
    } else {
        relative
            .trim_end_matches('/')
            .strip_suffix(".git")
            .unwrap_or(relative.trim_end_matches('/'))
    };
    split_project(project)?;
    Ok(project.into())
}
fn failure<T>(request: &Request, error: &intent_sourcecontrol::Error) -> Result<CheckoutResult<T>> {
    use intent_sourcecontrol::error::{AdmissionUnavailable, ProviderFailureKind};
    use intent_sourcecontrol::Error as Sc;
    let reason = match error {
        Sc::AdmissionUnavailable(
            AdmissionUnavailable::Missing | AdmissionUnavailable::Disconnected,
        )
        | Sc::NotConfigured(_) => CheckoutUnavailable::NotConnected,
        Sc::RateLimited(_) | Sc::AdmissionUnavailable(AdmissionUnavailable::Backoff) => {
            CheckoutUnavailable::RateLimited
        }
        Sc::Auth(_) => CheckoutUnavailable::AccessDenied,
        Sc::Provider(p) => match p.kind {
            ProviderFailureKind::CredentialRejected
            | ProviderFailureKind::ProjectDenied
            | ProviderFailureKind::ResourceDenied => CheckoutUnavailable::AccessDenied,
            _ => CheckoutUnavailable::Unreachable,
        },
        Sc::AdmissionRetired | Sc::AdmissionUnavailable(_) => CheckoutUnavailable::Retired,
        _ => CheckoutUnavailable::Unreachable,
    };
    request.public()?;
    Ok(CheckoutResult::unavailable(reason))
}

pub(crate) fn projects(
    services: &Services,
    q: CheckoutProjectsQuery,
) -> BoxFuture<'_, Result<CheckoutResult<CheckoutProjects>>> {
    entry(services, &CheckoutFrame::Projects(q.clone()), move |r| {
        Box::pin(async move {
            let (lease, connection) = r.bind(&q.checkout_id, &q.revision).await?;
            let scope = page_scope(None, q.query, q.limit, false)?;
            let page = lease.page(&scope, q.cursor.as_deref())?;
            let provider = match connection.provider() {
                Ok(provider) => provider,
                Err(error) => return failure(&r, &error),
            };
            let result = tokio::time::timeout(READ_LIMIT, async {
                if scope.query.is_empty() {
                    provider.list_repos(page).await
                } else {
                    provider.search_repos(&scope.query, page).await
                }
            })
            .await;
            let page = match result {
                Ok(Ok(page)) => page,
                Ok(Err(e)) => return failure(&r, &e),
                Err(_) => {
                    r.public()?;
                    return Ok(CheckoutResult::unavailable(
                        CheckoutUnavailable::Unreachable,
                    ));
                }
            };
            let items = page
                .items
                .into_iter()
                .map(|repo| {
                    project_wire(
                        connection.instance_base_url(),
                        repo,
                        lease.include_owner_avatar,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            r.private_projects(items.iter().map(|p| p.wire.project_path.clone()).collect())?;
            for project in &items {
                lease.remember(project.clone())?;
            }
            let next_cursor = lease.next_cursor(scope, page.next_cursor)?;
            Ok(CheckoutResult::Ready {
                value: CheckoutProjects {
                    items: items.into_iter().map(|p| p.wire).collect(),
                    next_cursor,
                },
            })
        })
    })
}

async fn load_project(
    lease: &Lease,
    connection: &Arc<GitlabCheckoutConnection>,
    path: &str,
) -> Result<std::result::Result<Project, intent_sourcecontrol::Error>> {
    let (owner, name) = split_project(path)?;
    connection.with_project_current(path, &mut || Ok(()))?;
    if let Some(project) = lease.projects.lock().map_err(denied)?.get(path).cloned() {
        return Ok(Ok(project));
    }
    let provider = match connection.provider() {
        Ok(provider) => provider,
        Err(error) => return Ok(Err(error)),
    };
    let result = tokio::time::timeout(READ_LIMIT, provider.get_repo(owner, name)).await;
    let repo = match result {
        Ok(Ok(repo)) => repo,
        Ok(Err(e)) => return Ok(Err(e)),
        Err(_) => {
            return Ok(Err(intent_sourcecontrol::Error::Api(
                "GitLab checkout read timed out".into(),
            )))
        }
    };
    let project = project_wire(
        connection.instance_base_url(),
        repo,
        lease.include_owner_avatar,
    )?;
    if project.wire.project_path != path {
        return Err(unavailable());
    }
    lease.remember(project.clone())?;
    Ok(Ok(project))
}

pub(crate) fn project(
    services: &Services,
    q: CheckoutProjectQuery,
) -> BoxFuture<'_, Result<CheckoutResult<CheckoutProjectDetail>>> {
    entry(services, &CheckoutFrame::Project(q.clone()), move |r| {
        Box::pin(async move {
            let (lease, connection) = r.bind(&q.checkout_id, &q.revision).await?;
            let (path, context_url) = match (q.project_path, q.url) {
                (Some(path), None) => (path, None),
                (None, Some(url)) => (
                    project_from_url(connection.instance_base_url(), &url)?,
                    Some(url),
                ),
                _ => {
                    return Err(Error::InvalidParams(
                        "supply exactly one projectPath or URL".into(),
                    ))
                }
            };
            let project = match load_project(&lease, &connection, &path).await? {
                Ok(p) => p,
                Err(e) => return failure(&r, &e),
            };
            r.private_projects(vec![path])?;
            Ok(CheckoutResult::Ready {
                value: CheckoutProjectDetail {
                    project: project.wire,
                    context_url,
                },
            })
        })
    })
}

pub(crate) fn branches(
    services: &Services,
    q: CheckoutBranchesQuery,
) -> BoxFuture<'_, Result<CheckoutResult<CheckoutBranches>>> {
    entry(services, &CheckoutFrame::Branches(q.clone()), move |r| {
        Box::pin(async move {
            let (lease, connection) = r.bind(&q.checkout_id, &q.revision).await?;
            let project = match load_project(&lease, &connection, &q.project_path).await? {
                Ok(p) => p,
                Err(e) => return failure(&r, &e),
            };
            r.private_projects(vec![q.project_path.clone()])?;
            let scope = page_scope(
                Some(q.project_path.clone()),
                q.query,
                q.limit,
                q.cached.unwrap_or(false),
            )?;
            let page = lease.page(&scope, q.cursor.as_deref())?;
            if scope.cached
                && page
                    .cursor
                    .as_deref()
                    .is_none_or(|c| c.starts_with("cache:"))
            {
                let root = services
                    .configured_worktrees_location()
                    .or_else(|| services.workspaces_root.clone())
                    .unwrap_or_else(crate::default_workspaces_root);
                let cache = connection.cache(
                    &intent_git::repo_cache::cache_root_for(&root),
                    &project.wire.clone_url,
                )?;
                if let Some(cached) = cache.branches().await? {
                    use sha2::{Digest, Sha256};
                    let mut digest = Sha256::new();
                    for branch in &cached.branches {
                        digest.update((branch.branch.len() as u64).to_be_bytes());
                        digest.update(branch.branch.as_bytes());
                        digest.update(branch.commit_sha.as_bytes());
                    }
                    let digest =
                        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest.finalize());
                    let start = match page.cursor.as_deref() {
                        None => 0,
                        Some(cursor) => cursor
                            .strip_prefix(&format!("cache:{digest}:"))
                            .and_then(|offset| offset.parse::<usize>().ok())
                            .ok_or_else(|| {
                                Error::InvalidParams(
                                    "cached branch cursor is no longer current".into(),
                                )
                            })?,
                    };
                    let matching = cached
                        .branches
                        .into_iter()
                        .filter(|branch| branch.branch.starts_with(&scope.query))
                        .collect::<Vec<_>>();
                    if start > matching.len() {
                        return Err(Error::InvalidParams("invalid cached branch cursor".into()));
                    }
                    let end = start
                        .saturating_add(usize::from(scope.limit))
                        .min(matching.len());
                    let items = {
                        let mut records = lease.branches.lock().map_err(denied)?;
                        if records.len() + end - start > MAX_RECORDS {
                            records.clear();
                        }
                        matching[start..end]
                            .iter()
                            .map(|branch| {
                                let key = (q.project_path.clone(), branch.branch.clone());
                                let protected = records
                                    .get(&key)
                                    .filter(|b| b.commit_sha == branch.commit_sha)
                                    .and_then(|b| b.protected);
                                let item = CheckoutBranch {
                                    name: branch.branch.clone(),
                                    commit_sha: branch.commit_sha.clone(),
                                    protected,
                                };
                                records.insert(key, item.clone());
                                item
                            })
                            .collect()
                    };
                    let next_cursor = lease.next_cursor(
                        scope,
                        (end < matching.len()).then(|| format!("cache:{digest}:{end}")),
                    )?;
                    return Ok(CheckoutResult::Ready {
                        value: CheckoutBranches {
                            items,
                            next_cursor,
                            default_branch: project.wire.default_branch,
                            cached: true,
                        },
                    });
                }
                if page.cursor.is_some() {
                    return Err(Error::InvalidParams(
                        "cached branch cursor has expired".into(),
                    ));
                }
            }
            let (owner, name) = split_project(&q.project_path)?;
            let provider = match connection.provider() {
                Ok(provider) => provider,
                Err(error) => return failure(&r, &error),
            };
            let result = tokio::time::timeout(
                READ_LIMIT,
                provider.list_remote_branches(
                    owner,
                    name,
                    (!scope.query.is_empty()).then_some(scope.query.as_str()),
                    page,
                ),
            )
            .await;
            let page = match result {
                Ok(Ok(page)) => page,
                Ok(Err(e)) => return failure(&r, &e),
                Err(_) => {
                    r.public()?;
                    return Ok(CheckoutResult::unavailable(
                        CheckoutUnavailable::Unreachable,
                    ));
                }
            };
            let items = page
                .items
                .into_iter()
                .map(|b| {
                    let sha = b
                        .commit_sha
                        .filter(|s| s.len() == 40 && s.bytes().all(|c| c.is_ascii_hexdigit()))
                        .ok_or_else(unavailable)?;
                    Ok(CheckoutBranch {
                        name: b.name,
                        commit_sha: sha,
                        protected: Some(b.protected),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            {
                let mut branches = lease.branches.lock().map_err(denied)?;
                if branches.len() + items.len() > MAX_RECORDS {
                    branches.clear();
                }
                for branch in &items {
                    branches.insert(
                        (q.project_path.clone(), branch.name.clone()),
                        branch.clone(),
                    );
                }
            }
            let next_cursor = lease.next_cursor(scope, page.next_cursor)?;
            Ok(CheckoutResult::Ready {
                value: CheckoutBranches {
                    items,
                    next_cursor,
                    default_branch: project.wire.default_branch,
                    cached: false,
                },
            })
        })
    })
}

pub(crate) fn warm(
    services: &Services,
    q: CheckoutSelection,
) -> BoxFuture<'_, Result<CheckoutResult<CheckoutWarm>>> {
    entry(services, &CheckoutFrame::Warm(q.clone()), move |r| {
        Box::pin(async move {
            let (lease, connection) = r.bind(&q.checkout_id, &q.revision).await?;
            let project = match load_project(&lease, &connection, &q.project_path).await? {
                Ok(p) => p,
                Err(e) => return failure(&r, &e),
            };
            let selection = create::checked_selection(&lease, &q)?;
            r.private_projects(vec![q.project_path.clone()])?;
            let root = services
                .configured_worktrees_location()
                .or_else(|| services.workspaces_root.clone())
                .unwrap_or_else(crate::default_workspaces_root);
            let cache = connection.cache(
                &intent_git::repo_cache::cache_root_for(&root),
                &project.wire.clone_url,
            )?;
            let credential = connection
                .native_credential(&project.wire.clone_url)
                .await?;
            // Cache owns its blocking worker and original request guard. No hit or
            // late completion can turn origin/freshness into authorization.
            cache.ensure(selection, Box::new(credential), None).await?;
            r.validate(&lease).await?;
            connection.with_project_current(&q.project_path, &mut || Ok(()))?;
            Ok(CheckoutResult::Ready {
                value: CheckoutWarm {
                    project_path: q.project_path,
                    branch: q.branch,
                    commit_sha: q.commit_sha,
                    cached: true,
                },
            })
        })
    })
}

pub(crate) fn release(
    services: &Services,
    q: CheckoutBinding,
) -> BoxFuture<'_, Result<CheckoutReleased>> {
    entry(services, &CheckoutFrame::Release(q.clone()), move |r| {
        Box::pin(async move {
            r.authority().await?;
            let lease = r
                .connection
                .checkout
                .leases
                .lock()
                .map_err(denied)?
                .get(&q.checkout_id)
                .cloned();
            if lease.as_ref().is_some_and(|l| l.revision != q.revision) {
                return Err(unavailable());
            }
            let released = lease.is_some();
            r.connection.checkout.retire(&q.checkout_id);
            r.public()?;
            Ok(CheckoutReleased { released })
        })
    })
}

#[path = "native_checkout/create.rs"]
pub(crate) mod create;

#[cfg(test)]
#[path = "native_checkout/tests.rs"]
mod tests;

#[path = "native_checkout/operations.rs"]
pub(crate) mod operations;

#[path = "native_checkout/repo_config.rs"]
mod repo_config;
pub(crate) use repo_config::read as repo_config;
