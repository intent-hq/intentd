//! The executor service: runs other daemons' (backends') remote agents
//! inside this regular daemon, over the executor link the daemon's own WSS
//! listener serves at `/node/link` and `/node/git`.
//!
//! Each executor credential (one per backend) gets its own storage under
//! `<data_dir>/executor/<credentialId>/` (journal, checkout caches, agent
//! records; the worktrees go in a short work dir, see [`storage`]), runner
//! and git sessions, opened when its backend first
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

use intent_core::link_traffic::LinkTraffic;
use intent_core::machines::{ExecutorCredential, ExecutorLiveStatus, ExecutorLiveness};
use intent_core::node_link::{
    LinkFrame, LinkHello, LinkWelcome, MachineCapabilities, ProviderCapability,
};
use intent_core::nodes::{NodeArch, NodeOs};
use intent_core::placement::AgentPlatform;
use intent_core::ExecutorCredentialId;
use intent_git::node_git::{CheckoutCache, NodeGit};
use intent_nodes::journal::Journal;
use intent_transport::node_link::{
    serve_helpers_with, ExecutorHost, HelperCredential, NodeConnection, NodeGitSessions, NodeLink,
};

mod checkout;
mod forge;
mod mcp_proxy;
mod runner;
mod scripts;
mod session;
mod setup;
mod skills;
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
    /// The machine's enabled providers may have changed: drop the ones now
    /// disabled from the cached list and add the ones now enabled with what
    /// is cheap to know, keeping what was observed of the others. Never
    /// blocks for long.
    fn reconcile(&self);
    /// The providers switched off in the machine's own `providers.enabled`,
    /// read from the current settings. Never blocks for long.
    fn disabled_providers(&self) -> Vec<String>;
}

/// The machine's `providers.enabled` as the capabilities report last
/// followed it. The loop compares values rather than reading a settings
/// change's paths: the settings watch keeps only the newest change, and a
/// change seen while the loop could not act must still be reported.
pub(crate) struct EnabledWatch(Option<std::collections::BTreeMap<String, bool>>);

impl EnabledWatch {
    pub(crate) fn new(current: Option<std::collections::BTreeMap<String, bool>>) -> Self {
        Self(current)
    }

    /// Whether `current` differs from what the report last followed; if so,
    /// it is what the report follows from now on.
    pub(crate) fn take_change(
        &mut self,
        current: Option<std::collections::BTreeMap<String, bool>>,
    ) -> bool {
        if current == self.0 {
            return false;
        }
        self.0 = current;
        true
    }
}

/// The cached provider list against `enabled_now` (every enabled provider
/// with its launch status only): in `enabled_now`'s order, each provider's
/// cached entry when there is one, else its launch-status entry.
pub(crate) fn reconcile_providers(
    cached: &[ProviderCapability],
    enabled_now: Vec<ProviderCapability>,
) -> Vec<ProviderCapability> {
    enabled_now
        .into_iter()
        .map(|fresh| {
            cached
                .iter()
                .find(|old| old.id == fresh.id)
                .cloned()
                .unwrap_or(fresh)
        })
        .collect()
}

/// What the executor service is built from.
pub(crate) struct ExecutorDeps {
    pub data_dir: PathBuf,
    /// The base of the agents' work dirs ([`storage::work_base`]); `None`
    /// keeps their homes in the data dir.
    pub work_base: Option<PathBuf>,
    /// This daemon's id: a bind must name it as `nodeId`.
    pub node_id: String,
    pub admission: Arc<dyn Admission>,
    pub launch: Arc<dyn MachineLaunch>,
    /// Runs the quick actions backends send (nodeProtocol 18).
    pub one_shot: Option<Arc<dyn runner::MachineOneShot>>,
    pub report: Arc<dyn MachineReport>,
    /// The intentd executable: the `git-remote-intent` helper and the
    /// agents' `mcp-bridge`.
    pub executable: PathBuf,
    /// The credentials still active in the store, read when the
    /// revocation feed lagged and may have dropped a revocation.
    pub active_credentials: ActiveCredentials,
    /// The daemon's link traffic capture: every backend's link is recorded
    /// in it while the owner captures.
    pub link_traffic: Option<Arc<LinkTraffic>>,
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
    /// The backend these agents run for, from its credential.
    owner: intent_services::RemoteOwner,
    git_sessions: NodeGitSessions,
    links: Vec<NodeLink>,
    sessions: Vec<tokio::task::JoinHandle<()>>,
    helpers: tokio::task::JoinHandle<()>,
    _storage: Arc<Storage>,
}

impl ExecutorService {
    /// The runners of the backends linked now (test seam).
    #[cfg(test)]
    fn runners_for_test(&self) -> Vec<Runner> {
        lock(&self.backends)
            .values()
            .map(|backend| backend.runner.clone())
            .collect()
    }
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
        let service = Arc::new(Self {
            deps,
            platform,
            make_checkouts,
            backends: Mutex::new(HashMap::new()),
            accepted: tx,
            worker: Mutex::new(None),
            refresher: Mutex::new(None),
            stopping: Mutex::new(Vec::new()),
            #[cfg(test)]
            revoke_pause: Mutex::new(None),
        });
        // caller-binding: allow — the executor service's own connection worker
        let worker = tokio::spawn(serve_connections(Arc::downgrade(&service), rx, revocations));
        *lock(&service.worker) = Some(worker);
        // caller-binding: allow — one-time startup cleanup of revoked credentials' storage
        tokio::spawn({
            let service = Arc::downgrade(&service);
            async move {
                if let Some(service) = service.upgrade() {
                    service.remove_inactive_storage().await;
                }
            }
        });
        Ok(service)
    }

    /// Stop every backend's agents (the daemon is stopping), each one's last
    /// work pushed before its backend's links close. Run it before the WSS
    /// listener stops, which closes those links.
    pub(crate) async fn shutdown(&self) {
        let tasks: Vec<_> = [lock(&self.worker).take(), lock(&self.refresher).take()]
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
        // Together, so the whole stop takes one drain, not one per backend.
        let backends: Vec<_> = lock(&self.backends)
            .drain()
            .map(|(_, backend)| {
                // caller-binding: allow — stops one backend's agents; shutdown joins it
                tokio::spawn(stop_backend_for_shutdown(backend))
            })
            .collect();
        for backend in backends {
            let _ = backend.await;
        }
        let stopping = std::mem::take(&mut *lock(&self.stopping));
        for task in stopping {
            let _ = task.await;
        }
    }

    /// The capabilities report: platform, version, the shared limits and
    /// their use, the cached provider list and the providers switched off
    /// in this machine's settings. Until the first full report
    /// lands, the list is primed with what is cheap to know, so a backend
    /// binding right after its first mint sees the providers.
    fn capabilities(&self) -> MachineCapabilities {
        self.deps.report.prime();
        let mut capabilities = MachineCapabilities::new(
            self.platform,
            env!("CARGO_PKG_VERSION"),
            0,
            intent_core::now_iso(),
        );
        capabilities.capacity = self.deps.admission.capacity();
        capabilities.providers = self.deps.report.providers();
        // An id is never in both lists, even while the cached list has not
        // caught up with a settings change yet.
        capabilities.disabled_providers = self
            .deps
            .report
            .disabled_providers()
            .into_iter()
            .filter(|id| !capabilities.providers.iter().any(|p| p.id == *id))
            .collect();
        capabilities
    }

    /// The machine's enabled providers changed: send every live backend
    /// link the reconciled report at once, then the fully refreshed one, so
    /// a backend stops offering a provider disabled here (or offers one
    /// enabled again) without waiting for its next capabilities request.
    pub(crate) async fn enabled_providers_changed(&self) {
        self.deps.report.reconcile();
        self.push_capabilities().await;
        self.deps.report.refresh().await;
        self.push_capabilities().await;
    }

    /// Send the current report, unasked, on every live backend link.
    async fn push_capabilities(&self) {
        let links: Vec<NodeLink> = lock(&self.backends)
            .values()
            .flat_map(|backend| backend.links.iter())
            .filter(|link| !link.is_closed())
            .cloned()
            .collect();
        if links.is_empty() {
            return;
        }
        let capabilities = self.capabilities();
        for link in links {
            let _ = link
                .send(&LinkFrame::CapabilitiesReply {
                    capabilities: capabilities.clone(),
                })
                .await;
        }
    }

    /// Open `credential`'s storage, journal and runner.
    async fn open_backend(&self, credential: &ExecutorCredential) -> anyhow::Result<Backend> {
        let owner = intent_services::RemoteOwner {
            backend_id: credential.backend_id.clone(),
            backend_name: credential.backend_name.clone(),
        };
        let credential = &credential.id;
        if !storage::valid_id(&credential.0) {
            bail!("executor credential id is not a safe path component");
        }
        let base = self.deps.data_dir.join(EXECUTOR_DIR);
        let root = base.join(&credential.0);
        let id = credential.0.clone();
        let work_base = self.deps.work_base.clone();
        let storage = Arc::new(
            tokio::task::spawn_blocking(move || {
                let socket = storage::git_socket_path(&base, &id, &storage::runtime_base())?;
                Ok::<_, anyhow::Error>(
                    Storage::open(&root, Some(socket))?.with_work_base(work_base.as_deref()),
                )
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
        let checkouts = (self.make_checkouts)(
            storage.clone(),
            CheckoutCache::new(
                storage.cache_dir(),
                NodeGit::new(helper_dir.clone(), socket.clone()),
            ),
        );
        let runner = Runner::new(RunnerConfig {
            one_shot: self.deps.one_shot.clone(),
            admission: Arc::new(Scoped {
                credential: credential.clone(),
                admission: self.deps.admission.clone(),
            }),
            launch: self.deps.launch.clone(),
            checkouts,
            journal,
            home: machine_home(),
            mcp_bridge_exe: self.deps.executable.clone(),
            git_socket: Some(socket),
            pull_staging: storage.pull_staging_dir(),
            forge_git: Some(runner::ForgeGit {
                helper_dir,
                sessions: git_sessions.clone(),
            }),
            auth_failed: {
                let report = self.deps.report.clone();
                Arc::new(move |provider: &str| report.launch_auth_failed(provider))
            },
        });
        // The live forge token for `intentd git-credential --node`
        // (nodeProtocol 15); the socket is the user's own.
        let forge = runner.forge_token();
        let credential: HelperCredential = Arc::new(move |host: &str| {
            (host == intent_core::node_link::FORGE_HOST_GITHUB)
                .then(|| forge.get())
                .flatten()
                .map(intent_core::node_link::SecretValue::from)
        });
        // caller-binding: allow — machine-local git helper socket for one backend's agents
        let helpers = tokio::spawn(serve_helpers_with(
            git_listener,
            git_sessions.clone(),
            Some(credential),
        ));
        Ok(Backend {
            runner,
            owner,
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
            match self.open_backend(&credential).await {
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
                )));
                // At every bind, the storage of credentials no longer active
                // (revoked, or unknown to this machine) goes; another
                // backend's active credential is never touched.
                let service = Arc::downgrade(self);
                // caller-binding: allow — node-local cleanup of revoked credentials' storage
                tokio::spawn(async move {
                    if let Some(service) = service.upgrade() {
                        service.remove_inactive_storage().await;
                    }
                });
            }
            NodeConnection::Git(git) => {
                tracing::info!("executor: backend's git connection bound");
                backend.git_sessions.attach(git);
            }
        }
    }

    /// Every running agent's provider process pid, tagged with the backend
    /// it runs for, for the memory sampler.
    pub(crate) fn agent_roots(&self) -> Vec<intent_services::RemoteAgentRoot> {
        lock(&self.backends)
            .values()
            .flat_map(|backend| backend.runner.agent_roots(&backend.owner))
            .collect()
    }

    /// How many backends hold an open inbound link to this machine now.
    pub(crate) fn linked_backends(&self) -> usize {
        lock(&self.backends)
            .values()
            .filter(|backend| backend.links.iter().any(|link| !link.is_closed()))
            .count()
    }

    /// Stop a revoked backend's agents and close its links, then (when
    /// `forget`: the credential is known to be revoked, not merely
    /// unreadable) remove its storage, checkouts included. The stop runs as
    /// a tracked task, so a shutdown racing it still waits for it.
    fn revoked(&self, credential: &ExecutorCredentialId, forget: bool) {
        let backend = lock(&self.backends).remove(credential);
        let root = (forget && storage::valid_id(&credential.0))
            .then(|| self.deps.data_dir.join(EXECUTOR_DIR).join(&credential.0));
        let work_base = self.deps.work_base.clone();
        #[cfg(test)]
        if backend.is_some() {
            let pause = lock(&self.revoke_pause).take();
            if let Some((removed, resume)) = pause {
                let _ = removed.send(());
                let _ = resume.recv();
            }
        }
        if backend.is_none() && root.is_none() {
            return;
        }
        tracing::info!("executor: credential revoked; stopping its agents");
        let mut stopping = lock(&self.stopping);
        stopping.retain(|task| !task.is_finished());
        // caller-binding: allow — stops one revoked backend's agents; shutdown joins it
        stopping.push(tokio::spawn(async move {
            if let Some(backend) = backend {
                stop_backend(backend).await;
            }
            if let Some(root) = root {
                remove_storage(root, work_base).await;
            }
        }));
    }

    /// At start: remove the storage of every credential that is no longer
    /// active (revoked, or unknown to the store), unless its backend is
    /// open. The directories are listed before the active credentials are
    /// read, so a credential minted and bound meanwhile is never touched.
    pub(crate) async fn remove_inactive_storage(&self) {
        let base = self.deps.data_dir.join(EXECUTOR_DIR);
        let listed: Vec<String> = match std::fs::read_dir(&base) {
            Ok(entries) => entries
                .filter_map(std::result::Result::ok)
                .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
                .filter_map(|entry| entry.file_name().into_string().ok())
                .filter(|name| storage::valid_id(name))
                .collect(),
            Err(_) => return,
        };
        let active = match (self.deps.active_credentials)().await {
            Ok(active) => active,
            Err(error) => {
                tracing::warn!(%error, "executor: cannot read executor credentials; keeping their storage");
                return;
            }
        };
        for name in listed {
            let id = ExecutorCredentialId::from(name.clone());
            if active.contains(&id) || lock(&self.backends).contains_key(&id) {
                continue;
            }
            tracing::info!("executor: removing a revoked credential's storage");
            remove_storage(base.join(name), self.deps.work_base.clone()).await;
        }
    }
}

/// Remove a revoked credential's storage root. Its agents' homes go
/// first; when one stays, the root (with the records naming them) stays
/// too, for the next purge.
async fn remove_storage(root: PathBuf, work_base: Option<PathBuf>) {
    let removed = tokio::task::spawn_blocking(move || {
        if !storage::remove_agent_homes(&root, work_base.as_deref()) {
            return Err(std::io::Error::other(
                "an agent's home could not be removed",
            ));
        }
        match std::fs::remove_dir_all(&root) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    })
    .await;
    if !matches!(removed, Ok(Ok(()))) {
        tracing::warn!("executor: could not remove a revoked credential's storage");
    }
}

impl ExecutorService {
    /// The revocation feed lagged: stop every backend whose credential is
    /// no longer active. If the store cannot say, stop them all (a
    /// still-valid backend simply binds again).
    async fn reconcile(&self) {
        let active = (self.deps.active_credentials)().await;
        let open: Vec<ExecutorCredentialId> = lock(&self.backends).keys().cloned().collect();
        for id in open {
            match &active {
                Ok(active) if active.contains(&id) => {}
                Ok(_) => self.revoked(&id, true),
                // Unknown, not revoked: stop, but keep its storage.
                Err(_) => self.revoked(&id, false),
            }
        }
    }
}

/// A revoked backend: close its links, settle its sessions, then stop its
/// agents, so none can be launched behind the stop.
async fn stop_backend(backend: Backend) {
    // The credential is no longer active: its agents' secrets go at once.
    backend.runner.head_link().secrets().clear();
    backend.runner.head_link().forge().clear();
    for link in &backend.links {
        link.close();
    }
    for session in backend.sessions {
        let _ = session.await;
    }
    backend.runner.shutdown().await;
    backend.helpers.abort();
}

/// The daemon is stopping: stop a backend's agents while its links are still
/// up, so each one's last work is pushed to the backend, then close the
/// links and settle their sessions. The runner closes its own admission
/// first, so nothing is launched behind the stop (intent-hq/intent#8437).
async fn stop_backend_for_shutdown(backend: Backend) {
    backend.runner.shutdown().await;
    for link in &backend.links {
        link.close();
    }
    for session in backend.sessions {
        let _ = session.await;
    }
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
                    service.revoked(&id, true);
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
    fn ready(&self) -> bool {
        true
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

    fn link_traffic(&self) -> Option<Arc<LinkTraffic>> {
        self.deps.link_traffic.clone()
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

    /// [`ExecutorService::agent_roots`]; none before the service is
    /// installed.
    pub(crate) fn agent_roots(&self) -> Vec<intent_services::RemoteAgentRoot> {
        self.0
            .get()
            .map(|service| service.agent_roots())
            .unwrap_or_default()
    }

    /// [`ExecutorService::linked_backends`]; none before the service is
    /// installed.
    pub(crate) fn linked_backends(&self) -> usize {
        self.0.get().map_or(0, |service| service.linked_backends())
    }

    #[cfg(test)]
    pub(crate) fn service(&self) -> Option<&Arc<ExecutorService>> {
        self.0.get()
    }
}

impl ExecutorHost for ExecutorSlot {
    /// No executor host until the service is installed.
    fn ready(&self) -> bool {
        self.0.get().is_some()
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

    fn link_traffic(&self) -> Option<Arc<LinkTraffic>> {
        self.0.get()?.link_traffic()
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

/// Quick actions run with the daemon's own one-shot route.
pub(crate) struct ServicesOneShot(pub intent_services::Services);

impl runner::MachineOneShot for ServicesOneShot {
    fn run(
        &self,
        frame: intent_core::node_link::OneShot,
        profile: Option<intent_acp::ProfileEnv>,
    ) -> intent_core::BoxFuture<'static, intent_core::node_link::OneShotReply> {
        let services = self.0.clone();
        Box::pin(async move { services.machine_one_shot(frame, profile).await })
    }
}

/// Providers resolve from the daemon's live settings, as a local spawn's.
/// The flag is whether this daemon may start a provider at all (`false` on
/// an orchestrator, which refuses every launch).
pub(crate) struct SettingsLaunch(pub Arc<intent_services::SettingsRegistry>, pub bool);

impl MachineLaunch for SettingsLaunch {
    fn resolve(&self, provider: &str) -> Result<ProviderLaunch, String> {
        if !self.1 {
            return Err("orchestrator-mode: this daemon runs no agents".into());
        }
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

/// How often the cached provider report is refreshed while a backend holds
/// an active executor credential.
const REPORT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10 * 60);

/// How often the report refresher checks whether any executor credential
/// is active, so a first mint gets a report without waiting a full
/// [`REPORT_INTERVAL`].
const REPORT_POLL: std::time::Duration = std::time::Duration::from_secs(30);

/// Whether the provider report is due: only while some executor credential
/// is active, right away on the first poll that sees one (`last` is reset
/// while none is), then every [`REPORT_INTERVAL`].
fn report_due(active: bool, last: Option<std::time::Instant>, now: std::time::Instant) -> bool {
    active && last.is_none_or(|at| now.saturating_duration_since(at) >= REPORT_INTERVAL)
}

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

    fn reconcile(&self) {
        let enabled_now = self.services.executor_provider_launch_statuses();
        let mut cache = lock(&self.cache);
        cache.0 = reconcile_providers(&cache.0, enabled_now);
    }

    fn disabled_providers(&self) -> Vec<String> {
        self.services.executor_disabled_providers()
    }
}

/// What [`start_in_daemon`] wires the executor service from.
pub(crate) struct DaemonParts<'a> {
    pub slot: &'a Arc<ExecutorSlot>,
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
    let service = match ExecutorService::start(
        ExecutorDeps {
            data_dir: parts.data_dir.to_owned(),
            work_base: storage::work_base(
                std::env::var_os(storage::WORK_DIR_ENV).as_deref(),
                machine_home().as_deref(),
            ),
            node_id,
            admission: Arc::new(RegistryAdmission(parts.manager.clone())),
            launch: Arc::new(SettingsLaunch(
                parts.settings.clone(),
                parts.services.may_spawn_provider_locally(),
            )),
            one_shot: Some(Arc::new(ServicesOneShot(parts.services.clone()))),
            report: report.clone(),
            executable,
            link_traffic: Some(parts.services.link_traffic().clone()),
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
    // Keep the provider report warm while any executor credential is
    // active; model discovery may run provider CLIs, so it never runs on a
    // bind, and never on a machine no backend was given access to. A change
    // of the enabled providers is reported to the bound backends at once.
    // caller-binding: allow — the executor service's background report refresh
    let refresher = tokio::spawn({
        let report = Arc::downgrade(&report);
        let active_credentials = service.deps.active_credentials.clone();
        let service = Arc::downgrade(&service);
        let registry = parts.settings.clone();
        let mut settings = registry.subscribe();
        let enabled_now = move || registry.snapshot().effective.providers.enabled.clone();
        let mut enabled = EnabledWatch::new(enabled_now());
        async move {
            let mut tick = tokio::time::interval(REPORT_POLL);
            let mut last = None;
            let mut watching = true;
            loop {
                tokio::select! {
                    _ = tick.tick() => {}
                    changed = settings.changed(), if watching => {
                        if changed.is_err() {
                            watching = false;
                        }
                    }
                }
                let Some(report) = report.upgrade() else {
                    return;
                };
                let active = match active_credentials().await {
                    Ok(active) => !active.is_empty(),
                    Err(error) => {
                        tracing::warn!(%error, "executor: cannot read executor credentials");
                        continue;
                    }
                };
                let enabled_changed = enabled.take_change(enabled_now());
                let now = std::time::Instant::now();
                if enabled_changed {
                    match service.upgrade() {
                        Some(service) if active => {
                            service.enabled_providers_changed().await;
                            last = Some(now);
                        }
                        Some(_) => report.reconcile(),
                        None => return,
                    }
                } else if !active {
                    last = None;
                } else if report_due(active, last, now) {
                    report.refresh().await;
                    last = Some(now);
                }
            }
        }
    });
    *lock(&service.refresher) = Some(refresher);
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
