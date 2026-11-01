//! The executor service: runs other daemons' (backends') remote agents
//! inside this regular daemon, over the executor link the daemon's own WSS
//! listener serves at `/node/link` and `/node/git`.
//!
//! Each executor credential (one per backend) gets its own storage under
//! `<data_dir>/executor/<credentialId>/` (journal, checkout caches,
//! worktrees), runner and git sessions, opened when its backend first
//! binds. Every remote agent counts against the machine's shared agent
//! limits (the process registry's cap and memory budget, shared with local
//! agents); a launch they deny is refused with `node-capacity`, never
//! queued. Providers resolve exactly as a local agent's do. Remote agents
//! never get workspace or agent rows in this daemon's store.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

use anyhow::bail;
use tokio::sync::mpsc;

use intent_core::machines::{ExecutorCredential, ExecutorLiveStatus, ExecutorLiveness};
use intent_core::node_link::{LinkHello, LinkWelcome, MachineCapabilities, ProviderCapability};
use intent_core::nodes::{NodeArch, NodeOs};
use intent_core::placement::AgentPlatform;
use intent_core::ExecutorCredentialId;
use intent_git::node_git::{CheckoutCache, NodeGit};
use intent_nodes::journal::Journal;
use intent_transport::node_link::{
    serve_helpers, ExecutorHost, NodeConnection, NodeGitSessions, NodeLink,
};

mod checkout;
mod runner;
mod session;
mod storage;
#[cfg(test)]
mod test_link;
mod workspace_api;

use runner::{Admission, MachineLaunch, ProviderLaunch};

use checkout::GitCheckouts;
use runner::{Runner, RunnerConfig};
use storage::Storage;

/// Where the executor service keeps its per-credential storage.
const EXECUTOR_DIR: &str = "executor";

/// What the machine reports about itself besides its agents. Reads are
/// cached and never block (the link server calls them under its lock).
pub(crate) trait MachineReport: Send + Sync {
    /// Each provider's status and models, as last observed.
    fn providers(&self) -> Vec<ProviderCapability>;
    /// The listener's direct hosts and tailcat address, as last observed.
    fn routes(&self) -> (Vec<String>, Option<String>);
    /// Observe the providers and routes again.
    fn refresh(&self) -> intent_core::BoxFuture<'_, ()>;
    /// Before the first full report: fill the provider list with what is
    /// cheap to know (launch resolution only). Never blocks for long.
    fn prime(&self) {}
    /// A launch without a profile failed because `provider`'s default
    /// account is not signed in.
    fn launch_auth_failed(&self, provider: &str);
}

/// What the executor service is built from.
pub(crate) struct ExecutorDeps {
    pub data_dir: PathBuf,
    /// This daemon's id: a bind must name it as `nodeId`.
    pub node_id: String,
    /// `remoteAgents.enabled`.
    pub enabled: Arc<dyn Fn() -> bool + Send + Sync>,
    pub admission: Arc<dyn Admission>,
    pub launch: Arc<dyn MachineLaunch>,
    pub report: Arc<dyn MachineReport>,
    /// The intentd executable: the `git-remote-intent` helper and the
    /// agents' `mcp-bridge`.
    pub executable: PathBuf,
    /// The credentials still active in the store, read when the
    /// revocation feed lagged and may have dropped a revocation.
    pub active_credentials: ActiveCredentials,
}

/// Reads the active executor credentials.
pub(crate) type ActiveCredentials = Arc<
    dyn Fn() -> intent_core::BoxFuture<
            'static,
            intent_core::Result<std::collections::HashSet<ExecutorCredentialId>>,
        > + Send
        + Sync,
>;

/// Builds a backend's checkouts from its storage and git cache.
type MakeCheckouts = fn(Arc<Storage>, CheckoutCache) -> Arc<dyn checkout::Checkouts>;

/// One backend's agents on this machine.
struct Backend {
    runner: Runner,
    git_sessions: NodeGitSessions,
    links: Vec<NodeLink>,
    sessions: Vec<tokio::task::JoinHandle<()>>,
    helpers: tokio::task::JoinHandle<()>,
    _storage: Arc<Storage>,
}

/// The running executor service. Cloning the `Arc` shares it.
pub(crate) struct ExecutorService {
    deps: ExecutorDeps,
    platform: AgentPlatform,
    make_checkouts: MakeCheckouts,
    backends: Mutex<HashMap<ExecutorCredentialId, Backend>>,
    accepted: mpsc::UnboundedSender<(ExecutorCredential, NodeConnection)>,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
    refresher: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Revoked backends being stopped; shutdown waits for them, so a
    /// backend taken out of `backends` is never abandoned half-stopped.
    stopping: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    /// The link server, to close live connections when the setting turns
    /// off. Weak: it holds this service as its host.
    links: OnceLock<std::sync::Weak<intent_transport::node_link::ExecutorLinks>>,
    /// `remoteAgents.enabled` at the last settings change.
    was_enabled: std::sync::atomic::AtomicBool,
    /// Follows settings changes (daemon only).
    watcher: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Test seam: `revoked()` signals the first sender once it has taken
    /// the backend out, then waits on the receiver before registering its
    /// stop.
    #[cfg(test)]
    revoke_pause: Mutex<Option<RevokePause>>,
}

#[cfg(test)]
type RevokePause = (
    std::sync::mpsc::SyncSender<()>,
    std::sync::mpsc::Receiver<()>,
);

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl ExecutorService {
    /// Start the service. `revocations` (the store's revocation feed) stops
    /// a revoked backend's agents.
    pub(crate) fn start(
        deps: ExecutorDeps,
        revocations: Option<tokio::sync::broadcast::Receiver<ExecutorCredentialId>>,
    ) -> anyhow::Result<Arc<Self>> {
        Self::start_with_checkouts(deps, revocations, |storage, cache| {
            Arc::new(GitCheckouts::new(cache, storage))
        })
    }

    fn start_with_checkouts(
        deps: ExecutorDeps,
        revocations: Option<tokio::sync::broadcast::Receiver<ExecutorCredentialId>>,
        make_checkouts: MakeCheckouts,
    ) -> anyhow::Result<Arc<Self>> {
        let platform = host_platform()?;
        let (tx, rx) = mpsc::unbounded_channel();
        let was_enabled = std::sync::atomic::AtomicBool::new((deps.enabled)());
        let service = Arc::new(Self {
            deps,
            platform,
            make_checkouts,
            backends: Mutex::new(HashMap::new()),
            accepted: tx,
            worker: Mutex::new(None),
            refresher: Mutex::new(None),
            stopping: Mutex::new(Vec::new()),
            links: OnceLock::new(),
            was_enabled,
            watcher: Mutex::new(None),
            #[cfg(test)]
            revoke_pause: Mutex::new(None),
        });
        // caller-binding: allow — the executor service's own connection worker
        let worker = tokio::spawn(serve_connections(Arc::downgrade(&service), rx, revocations));
        *lock(&service.worker) = Some(worker);
        Ok(service)
    }

    /// The link server serving this service, so turning the setting off
    /// can close its live connections.
    pub(crate) fn attach_links(&self, links: &Arc<intent_transport::node_link::ExecutorLinks>) {
        let _ = self.links.set(Arc::downgrade(links));
    }

    /// Settings changed: when `remoteAgents.enabled` turned off, close every
    /// live executor connection (links and git). New upgrades are already
    /// refused while it is off; after it turns on again a backend binds
    /// afresh. Remote agents keep running, as after a link loss.
    pub(crate) async fn on_settings_changed(&self) {
        let enabled = (self.deps.enabled)();
        let was = self
            .was_enabled
            .swap(enabled, std::sync::atomic::Ordering::SeqCst);
        if !was && enabled {
            // A backend may bind right away: report what is cheap to know
            // now and observe the providers fully, not at the next periodic
            // refresh.
            self.deps.report.prime();
            let report = self.deps.report.clone();
            // caller-binding: allow — the executor service's report refresh on enable
            tokio::spawn(async move { report.refresh().await });
        }
        if was && !enabled {
            tracing::info!("executor: remote agents disabled; closing live executor links");
            if let Some(links) = self.links.get().and_then(std::sync::Weak::upgrade) {
                links.close_all().await;
            }
            for backend in lock(&self.backends).values_mut() {
                for link in backend.links.drain(..) {
                    link.close();
                }
            }
        }
    }

    /// Stop every backend's agents (the daemon is stopping).
    pub(crate) async fn shutdown(&self) {
        let tasks: Vec<_> = [
            lock(&self.worker).take(),
            lock(&self.refresher).take(),
            lock(&self.watcher).take(),
        ]
        .into_iter()
        .flatten()
        .collect();
        // Abort, then wait until each task has actually stopped: a poll
        // already running on another thread finishes first, so a worker
        // midway through `revoked()` has registered its stop before the
        // collections below are drained.
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            let _ = task.await;
        }
        let backends: Vec<Backend> = lock(&self.backends).drain().map(|(_, b)| b).collect();
        for backend in backends {
            stop_backend(backend).await;
        }
        let stopping = std::mem::take(&mut *lock(&self.stopping));
        for task in stopping {
            let _ = task.await;
        }
    }

    /// The capabilities report: platform, version, the shared limits and
    /// their use, and the cached provider list.
    fn capabilities(&self) -> MachineCapabilities {
        let mut capabilities = MachineCapabilities::new(
            self.platform,
            env!("CARGO_PKG_VERSION"),
            0,
            intent_core::now_iso(),
        );
        capabilities.capacity = self.deps.admission.capacity();
        capabilities.providers = self.deps.report.providers();
        capabilities
    }

    /// Open `credential`'s storage, journal and runner.
    async fn open_backend(&self, credential: &ExecutorCredentialId) -> anyhow::Result<Backend> {
        if !storage::valid_id(&credential.0) {
            bail!("executor credential id is not a safe path component");
        }
        let base = self.deps.data_dir.join(EXECUTOR_DIR);
        let root = base.join(&credential.0);
        let id = credential.0.clone();
        let storage = Arc::new(
            tokio::task::spawn_blocking(move || {
                let socket = storage::git_socket_path(&base, &id, &storage::runtime_base())?;
                Storage::open(&root, Some(socket))
            })
            .await??,
        );
        let socket = storage.git_socket();
        let journal_dir = storage.journal_dir();
        let journal = tokio::task::spawn_blocking(move || Journal::open(&journal_dir)).await??;
        let helper_dir = storage.helper_dir();
        NodeGit::install_helper(&helper_dir, &self.deps.executable)
            .map_err(|e| anyhow::anyhow!("install the git-remote-intent helper: {e}"))?;
        match std::fs::remove_file(&socket) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => bail!("remove the stale git socket {}: {e}", socket.display()),
        }
        let git_listener = tokio::net::UnixListener::bind(&socket)
            .map_err(|e| anyhow::anyhow!("bind the git socket {}: {e}", socket.display()))?;
        let git_sessions = NodeGitSessions::new();
        // caller-binding: allow — machine-local git helper socket for one backend's agents
        let helpers = tokio::spawn(serve_helpers(git_listener, git_sessions.clone()));
        let checkouts = (self.make_checkouts)(
            storage.clone(),
            CheckoutCache::new(storage.cache_dir(), NodeGit::new(helper_dir, socket)),
        );
        let runner = Runner::new(RunnerConfig {
            admission: Arc::new(Scoped {
                credential: credential.clone(),
                admission: self.deps.admission.clone(),
            }),
            launch: self.deps.launch.clone(),
            checkouts,
            journal,
            home: machine_home(),
            mcp_bridge_exe: self.deps.executable.clone(),
            auth_failed: {
                let report = self.deps.report.clone();
                Arc::new(move |provider: &str| report.launch_auth_failed(provider))
            },
        });
        Ok(Backend {
            runner,
            git_sessions,
            links: Vec::new(),
            sessions: Vec::new(),
            helpers,
            _storage: storage,
        })
    }

    /// Hand an admitted connection to its backend, opening it first.
    async fn take(self: &Arc<Self>, credential: ExecutorCredential, connection: NodeConnection) {
        let opened = lock(&self.backends).contains_key(&credential.id);
        if !opened {
            match self.open_backend(&credential.id).await {
                Ok(backend) => {
                    lock(&self.backends).insert(credential.id.clone(), backend);
                }
                Err(error) => {
                    tracing::warn!(%error, "executor: backend storage could not be opened");
                    close(connection);
                    return;
                }
            }
        }
        let mut backends = lock(&self.backends);
        let Some(backend) = backends.get_mut(&credential.id) else {
            close(connection);
            return;
        };
        match connection {
            NodeConnection::Link {
                link,
                events,
                hello,
            } => {
                tracing::info!(
                    generation = hello.binding.generation.0,
                    "executor: backend bound"
                );
                backend.links.retain(|old| !old.is_closed());
                backend.links.push(link.clone());
                backend.sessions.retain(|s| !s.is_finished());
                let report: session::Report = {
                    let service = Arc::downgrade(self);
                    let platform = self.platform;
                    Arc::new(move || {
                        let service = service.clone();
                        Box::pin(async move {
                            match service.upgrade() {
                                Some(service) => {
                                    service.deps.report.refresh().await;
                                    service.capabilities()
                                }
                                // The daemon is stopping.
                                None => MachineCapabilities::new(
                                    platform,
                                    env!("CARGO_PKG_VERSION"),
                                    0,
                                    intent_core::now_iso(),
                                ),
                            }
                        })
                    })
                };
                // caller-binding: allow — one bound executor link's session
                backend.sessions.push(tokio::spawn(session::serve_link(
                    backend.runner.clone(),
                    link,
                    events,
                    hello,
                    report,
                    self.deps.enabled.clone(),
                )));
            }
            NodeConnection::Git(git) => {
                tracing::info!("executor: backend's git connection bound");
                backend.git_sessions.attach(git);
            }
        }
    }

    /// Stop a revoked backend's agents and close its links. The stop runs
    /// as a tracked task, so a shutdown racing it still waits for it.
    fn revoked(&self, credential: &ExecutorCredentialId) {
        let backend = lock(&self.backends).remove(credential);
        #[cfg(test)]
        if backend.is_some() {
            let pause = lock(&self.revoke_pause).take();
            if let Some((removed, resume)) = pause {
                let _ = removed.send(());
                let _ = resume.recv();
            }
        }
        if let Some(backend) = backend {
            tracing::info!("executor: credential revoked; stopping its agents");
            let mut stopping = lock(&self.stopping);
            stopping.retain(|task| !task.is_finished());
            // caller-binding: allow — stops one revoked backend's agents; shutdown joins it
            stopping.push(tokio::spawn(stop_backend(backend)));
        }
    }

    /// The revocation feed lagged: stop every backend whose credential is
    /// no longer active. If the store cannot say, stop them all (a
    /// still-valid backend simply binds again).
    async fn reconcile(&self) {
        let active = (self.deps.active_credentials)().await;
        let open: Vec<ExecutorCredentialId> = lock(&self.backends).keys().cloned().collect();
        for id in open {
            let keep = match &active {
                Ok(active) => active.contains(&id),
                Err(_) => false,
            };
            if !keep {
                self.revoked(&id);
            }
        }
    }
}

/// Close a backend's links, settle its sessions, then stop its agents, so
/// none can be launched behind the stop.
async fn stop_backend(backend: Backend) {
    for link in &backend.links {
        link.close();
    }
    for session in backend.sessions {
        let _ = session.await;
    }
    backend.runner.shutdown().await;
    backend.helpers.abort();
}

fn close(connection: NodeConnection) {
    match connection {
        NodeConnection::Link { link, .. } => link.close(),
        NodeConnection::Git(git) => git.close(),
    }
}

/// Take admitted connections in order, and stop revoked backends.
async fn serve_connections(
    service: std::sync::Weak<ExecutorService>,
    mut accepted: mpsc::UnboundedReceiver<(ExecutorCredential, NodeConnection)>,
    mut revocations: Option<tokio::sync::broadcast::Receiver<ExecutorCredentialId>>,
) {
    loop {
        tokio::select! {
            next = accepted.recv() => {
                let Some((credential, connection)) = next else { return };
                let Some(service) = service.upgrade() else { return };
                service.take(credential, connection).await;
            }
            revoked = async {
                match revocations.as_mut() {
                    Some(feed) => feed.recv().await,
                    None => std::future::pending().await,
                }
            } => match revoked {
                Ok(id) => {
                    let Some(service) = service.upgrade() else { return };
                    service.revoked(&id);
                }
                // Revocations were dropped; re-read which credentials are
                // still active.
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    let Some(service) = service.upgrade() else { return };
                    service.reconcile().await;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => revocations = None,
            },
        }
    }
}

impl ExecutorHost for ExecutorService {
    fn enabled(&self) -> bool {
        (self.deps.enabled)()
    }

    fn node_id(&self) -> String {
        self.deps.node_id.clone()
    }

    fn welcome(&self, credential: &ExecutorCredential, _hello: &LinkHello) -> LinkWelcome {
        let running_agents = lock(&self.backends)
            .get(&credential.id)
            .map(|backend| backend.runner.running())
            .unwrap_or_default();
        let (hosts, tc_address) = self.deps.report.routes();
        LinkWelcome {
            protocol: 0,
            node_id: String::new(),
            running_agents,
            capabilities: self.capabilities(),
            hosts,
            tc_address,
        }
    }

    fn accept(&self, credential: ExecutorCredential, connection: NodeConnection) {
        if let Err(mpsc::error::SendError((_, connection))) =
            self.accepted.send((credential, connection))
        {
            close(connection);
        }
    }
}

impl ExecutorLiveness for ExecutorService {
    fn status(&self, credential_id: &ExecutorCredentialId) -> ExecutorLiveStatus {
        lock(&self.backends)
            .get(credential_id)
            .map(|backend| ExecutorLiveStatus {
                connected: backend.links.iter().any(|link| !link.is_closed()),
                running_agents: u32::try_from(backend.runner.running().len()).unwrap_or(u32::MAX),
            })
            .unwrap_or_default()
    }
}

/// The executor host the daemon's listeners are built with before the
/// service exists (the listener starts before the store is ready). Until
/// the service is installed, `/node/*` is refused as disabled.
#[derive(Default)]
pub(crate) struct ExecutorSlot(OnceLock<Arc<ExecutorService>>);

impl ExecutorSlot {
    /// Install the service; later calls are ignored.
    pub(crate) fn install(&self, service: Arc<ExecutorService>) {
        let _ = self.0.set(service);
    }

    #[cfg(test)]
    pub(crate) fn service(&self) -> Option<&Arc<ExecutorService>> {
        self.0.get()
    }
}

impl ExecutorHost for ExecutorSlot {
    fn enabled(&self) -> bool {
        self.0.get().is_some_and(|service| service.enabled())
    }

    fn node_id(&self) -> String {
        self.0
            .get()
            .map(|service| ExecutorHost::node_id(service.as_ref()))
            .unwrap_or_default()
    }

    fn welcome(&self, credential: &ExecutorCredential, hello: &LinkHello) -> LinkWelcome {
        match self.0.get() {
            Some(service) => service.welcome(credential, hello),
            // Unreachable: a disabled host admits no bind.
            None => LinkWelcome {
                protocol: 0,
                node_id: String::new(),
                running_agents: Vec::new(),
                capabilities: MachineCapabilities::new(
                    host_platform().unwrap_or(AgentPlatform {
                        os: NodeOs::Linux,
                        arch: NodeArch::X86_64,
                    }),
                    env!("CARGO_PKG_VERSION"),
                    0,
                    intent_core::now_iso(),
                ),
                hosts: Vec::new(),
                tc_address: None,
            },
        }
    }

    fn accept(&self, credential: ExecutorCredential, connection: NodeConnection) {
        match self.0.get() {
            Some(service) => service.accept(credential, connection),
            None => close(connection),
        }
    }
}

impl ExecutorLiveness for ExecutorSlot {
    fn status(&self, credential_id: &ExecutorCredentialId) -> ExecutorLiveStatus {
        self.0
            .get()
            .map(|service| service.status(credential_id))
            .unwrap_or_default()
    }
}

/// A backend's view of the shared limits: its agents' slots are keyed by
/// credential, so no backend can release a local agent's slot or another
/// backend's by naming its agent id.
struct Scoped {
    credential: ExecutorCredentialId,
    admission: Arc<dyn Admission>,
}

impl Scoped {
    fn key(&self, agent_id: &intent_core::AgentId) -> intent_core::AgentId {
        intent_core::AgentId::from_string(format!("executor:{}:{}", self.credential.0, agent_id.0))
    }
}

impl Admission for Scoped {
    fn try_admit(
        &self,
        agent_id: &intent_core::AgentId,
    ) -> Result<(), intent_core::placement::CapacityRefusal> {
        self.admission.try_admit(&self.key(agent_id))
    }
    fn release(&self, agent_id: &intent_core::AgentId) {
        self.admission.release(&self.key(agent_id));
    }
    fn capacity(&self) -> intent_core::node_link::MachineCapacity {
        self.admission.capacity()
    }
}

/// The daemon's own limits: its process registry (cap and memory budget),
/// shared with local agents.
pub(crate) struct RegistryAdmission(pub Arc<intent_services::AgentManager>);

impl Admission for RegistryAdmission {
    fn try_admit(
        &self,
        agent_id: &intent_core::AgentId,
    ) -> Result<(), intent_core::placement::CapacityRefusal> {
        self.0.registry().try_admit(agent_id)
    }
    fn release(&self, agent_id: &intent_core::AgentId) {
        self.0.registry().deregister(agent_id);
    }
    fn capacity(&self) -> intent_core::node_link::MachineCapacity {
        let registry = self.0.registry();
        let budget = registry.budget_status();
        intent_core::node_link::MachineCapacity {
            running_agents: u32::try_from(registry.size()).unwrap_or(u32::MAX),
            agent_cap: u32::try_from(registry.cap()).unwrap_or(u32::MAX),
            charged_memory_bytes: budget.and_then(|(_, charged, _)| charged).unwrap_or(0),
            memory_budget_bytes: budget.map(|(bytes, _, _)| bytes),
        }
    }
}

/// Providers resolve from the daemon's live settings, as a local spawn's.
pub(crate) struct SettingsLaunch(pub Arc<intent_services::SettingsRegistry>);

impl MachineLaunch for SettingsLaunch {
    fn resolve(&self, provider: &str) -> Result<ProviderLaunch, String> {
        let snapshot = self.0.snapshot();
        match intent_services::resolve_provider_launch(provider, &snapshot.effective) {
            Ok(spec) => Ok(ProviderLaunch {
                config: spec.provider,
                binary: spec.provider_binary,
                npx_fallback_binary: spec.npx_fallback_binary,
                npx_fallback_package: spec.npx_fallback_package,
                extra_env: spec.extra_env,
            }),
            Err(intent_core::Error::NotFound(_)) => Err("unknown or disabled".into()),
            Err(_) => Err("its runtime is not installed".into()),
        }
    }

    fn policy(&self) -> intent_acp::PermissionPolicy {
        use intent_core::settings_file::RemoteAgentPermission;
        match self.0.snapshot().effective.remote_agents.permission {
            RemoteAgentPermission::AllowAll => intent_acp::PermissionPolicy::AllowAll,
            RemoteAgentPermission::AutoByRisk => intent_acp::PermissionPolicy::AutoByRisk,
            RemoteAgentPermission::DenyAll => intent_acp::PermissionPolicy::DenyAll,
        }
    }
}

/// How often the cached provider report is refreshed while the executor
/// link is enabled.
const REPORT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10 * 60);

/// The daemon's [`MachineReport`]: its provider capabilities (launch
/// resolution, `models.list` and sign-in probes) and its listener's routes,
/// cached.
pub(crate) struct DaemonReport {
    services: intent_services::Services,
    routes: Option<Arc<dyn intent_core::machines::MachineRoutesSource>>,
    cache: Mutex<(Vec<ProviderCapability>, Vec<String>, Option<String>)>,
    /// When each provider's default account last failed a launch, until a
    /// refresh's newer conclusive sign-in probe clears it.
    auth_failures: Mutex<HashMap<String, std::time::Instant>>,
    /// Single-flights refreshes.
    refreshing: tokio::sync::Mutex<()>,
}

/// Report `id` as `auth-failed`, unless it cannot launch at all.
fn mark_auth_failed(providers: &mut [ProviderCapability], id: &str) {
    use intent_core::node_link::ProviderStatus;
    for provider in providers.iter_mut().filter(|p| p.id == id) {
        if !matches!(
            provider.status,
            ProviderStatus::CliMissing | ProviderStatus::PrerequisiteMissing
        ) {
            provider.status = ProviderStatus::AuthFailed;
        }
    }
}

/// After a refresh built `providers` from the failures in `seen`: forget
/// each failure the refresh cleared, and report as failed every one that
/// arrived while it ran.
fn settle_auth_failures(
    providers: &mut [ProviderCapability],
    failures: &mut HashMap<String, std::time::Instant>,
    seen: &HashMap<String, std::time::Instant>,
) {
    use intent_core::node_link::ProviderStatus;
    failures.retain(|id, at| {
        if seen.get(id) != Some(at) {
            mark_auth_failed(providers, id);
            return true;
        }
        providers
            .iter()
            .any(|p| p.id == *id && p.status == ProviderStatus::AuthFailed)
    });
}

impl MachineReport for DaemonReport {
    fn providers(&self) -> Vec<ProviderCapability> {
        lock(&self.cache).0.clone()
    }

    fn routes(&self) -> (Vec<String>, Option<String>) {
        let cache = lock(&self.cache);
        (cache.1.clone(), cache.2.clone())
    }

    fn refresh(&self) -> intent_core::BoxFuture<'_, ()> {
        Box::pin(async move {
            self.prime();
            let _single = self.refreshing.lock().await;
            let seen = lock(&self.auth_failures).clone();
            let mut providers = self.services.executor_provider_capabilities(&seen).await;
            let routes = match &self.routes {
                Some(source) => source.machine_routes().await.ok(),
                None => None,
            };
            // Failures before cache: the order `launch_auth_failed` takes.
            let mut failures = lock(&self.auth_failures);
            settle_auth_failures(&mut providers, &mut failures, &seen);
            let mut cache = lock(&self.cache);
            drop(failures);
            cache.0 = providers;
            if let Some(routes) = routes {
                cache.1 = routes.hosts;
                cache.2 = routes.tc_address;
            }
        })
    }

    /// The full report can take minutes (sign-in probes, model listing
    /// through provider adapters); until the first one lands, report what
    /// each provider's launch resolution says.
    fn prime(&self) {
        let mut cache = lock(&self.cache);
        if cache.0.is_empty() {
            cache.0 = self.services.executor_provider_launch_statuses();
        }
    }

    fn launch_auth_failed(&self, provider: &str) {
        let mut failures = lock(&self.auth_failures);
        failures.insert(provider.to_owned(), std::time::Instant::now());
        let mut cache = lock(&self.cache);
        drop(failures);
        mark_auth_failed(&mut cache.0, provider);
    }
}

/// What [`start_in_daemon`] wires the executor service from.
pub(crate) struct DaemonParts<'a> {
    pub slot: &'a Arc<ExecutorSlot>,
    pub links: &'a Arc<intent_transport::node_link::ExecutorLinks>,
    pub services: &'a intent_services::Services,
    pub manager: &'a Arc<intent_services::AgentManager>,
    pub settings: &'a Arc<intent_services::SettingsRegistry>,
    pub data_dir: &'a std::path::Path,
    pub routes: Option<Arc<dyn intent_core::machines::MachineRoutesSource>>,
}

/// Start the executor service, install it behind the listeners' slot and
/// report its live state to `executor.list`. A failure leaves the link
/// refused as disabled and is logged; the daemon keeps running.
pub(crate) async fn start_in_daemon(parts: DaemonParts<'_>) -> Option<Arc<ExecutorService>> {
    use intent_core::WorkspaceApi as _;
    let node_id = match parts.services.store().daemon_id().await {
        Ok(id) => id,
        Err(error) => {
            tracing::warn!(%error, "executor: no daemon id; remote agents are unavailable");
            return None;
        }
    };
    let executable = match std::env::current_exe() {
        Ok(path) => path,
        Err(error) => {
            tracing::warn!(%error, "executor: cannot locate intentd; remote agents are unavailable");
            return None;
        }
    };
    let report = Arc::new(DaemonReport {
        services: parts.services.clone(),
        routes: parts.routes,
        cache: Mutex::new((Vec::new(), Vec::new(), None)),
        auth_failures: Mutex::new(HashMap::new()),
        refreshing: tokio::sync::Mutex::new(()),
    });
    let enabled: Arc<dyn Fn() -> bool + Send + Sync> = {
        let settings = parts.settings.clone();
        Arc::new(move || settings.snapshot().effective.remote_agents.enabled)
    };
    let service = match ExecutorService::start(
        ExecutorDeps {
            data_dir: parts.data_dir.to_owned(),
            node_id,
            enabled: enabled.clone(),
            admission: Arc::new(RegistryAdmission(parts.manager.clone())),
            launch: Arc::new(SettingsLaunch(parts.settings.clone())),
            report: report.clone(),
            executable,
            active_credentials: {
                let store = parts.services.store().clone();
                Arc::new(move || {
                    let store = store.clone();
                    Box::pin(async move {
                        Ok(store
                            .list_active_executor_credentials()
                            .await?
                            .into_iter()
                            .map(|credential| credential.id)
                            .collect())
                    })
                })
            },
        },
        parts.services.subscribe_executor_revocations(),
    ) {
        Ok(service) => service,
        Err(error) => {
            tracing::warn!(%error, "executor: remote agents are unavailable on this machine");
            return None;
        }
    };
    // Keep the provider report warm while the link is enabled; model
    // discovery may run provider CLIs, so it never runs on a bind.
    // caller-binding: allow — the executor service's background report refresh
    let refresher = tokio::spawn({
        let report = Arc::downgrade(&report);
        async move {
            let mut tick = tokio::time::interval(REPORT_INTERVAL);
            loop {
                tick.tick().await;
                let Some(report) = report.upgrade() else {
                    return;
                };
                if enabled() {
                    report.refresh().await;
                }
            }
        }
    });
    *lock(&service.refresher) = Some(refresher);
    service.attach_links(parts.links);
    // Turning `remoteAgents.enabled` off closes live executor connections.
    // caller-binding: allow — the executor service's settings follower
    let watcher = tokio::spawn({
        let mut changes = parts.settings.subscribe();
        let service = Arc::downgrade(&service);
        async move {
            while changes.changed().await.is_ok() {
                let Some(service) = service.upgrade() else {
                    return;
                };
                service.on_settings_changed().await;
            }
        }
    });
    *lock(&service.watcher) = Some(watcher);
    parts.slot.install(service.clone());
    parts.services.attach_executor_liveness(parts.slot.clone());
    Some(service)
}

/// The machine user's home directory, which a leading `~` in a provider
/// profile's account directory refers to.
fn machine_home() -> Option<std::path::PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .filter(|home| !home.is_empty())
        .map(std::path::PathBuf::from)
}

/// The platform this machine runs on, as a backend's placement sees it.
fn host_platform() -> anyhow::Result<AgentPlatform> {
    platform_of(std::env::consts::OS, std::env::consts::ARCH)
}

fn platform_of(os: &str, arch: &str) -> anyhow::Result<AgentPlatform> {
    let os = match os {
        "linux" => NodeOs::Linux,
        "macos" => NodeOs::Macos,
        other => bail!("remote agents run on Linux and macOS, not {other}"),
    };
    let arch = match arch {
        "x86_64" => NodeArch::X86_64,
        "aarch64" => NodeArch::Aarch64,
        other => bail!("remote agents run on x86_64 and aarch64, not {other}"),
    };
    Ok(AgentPlatform { os, arch })
}

#[cfg(test)]
mod tests;
