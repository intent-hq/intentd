//! Remote agents for intentd: the backend side of daemon-to-daemon remote
//! agents.
//!
//! The owner exposes a machine to this backend (`remoteMachines.add`, after
//! the machine minted an executor credential for it). The backend keeps the
//! machine's routes and certificate pin in its store and the credential's
//! token in its secret store, and connects to the machine's executor link
//! only when it is needed: to place or drive an agent there, or to refresh
//! its capabilities. A link with nothing to do is closed after an idle
//! timeout; a link lost while agents are live there is redialed, and the
//! hello's `ackedSeq` makes the machine replay what this backend has not
//! ingested.
//!
//! Each machine's connector ingests its journal in sequence order, hands
//! each record to the [`RemoteAgentRuntime`] of its agent, persists the
//! highest contiguous sequence and only then acks it, so a reconnect (or a
//! backend restart) replays exactly the records not yet ingested.
//!
//! [`RemoteAgentRuntime`] is the agent manager's `AgentRuntime` for an agent
//! running on a machine: prompts, cancels and stops go out as link frames,
//! and the machine's journal comes back as the runtime's notifications and
//! turn outcomes.
//!
//! [`RemoteLaunch`] is the feature's remote-launch port: admission against
//! the exposed machines, placing a created agent on one, the runtime that
//! starts it there on its first turn, and the `remoteMachines.*` methods.
//!
//! The feature sits above `intent-services`; the composition root registers
//! it with [`register`] and hands it the tailcat binary with
//! [`RemoteServicesExt::configure_remote_machines`]. No daemon setting
//! gates it: the frontend's Labs flag decides whether a user sees it, and
//! the model is told about remote placement only while a machine is
//! exposed to this backend. In node mode no connector ever starts.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use intent_core::{Error, Result};
use intent_git::ref_storage::{GitStorage, LocalGitStorage};
use intent_node_connections::{InProcessNodeConnections, NodeConnections};
use intent_services::feature_api::Services;

mod claim;
mod cloud;
mod connect {
    pub(crate) use intent_node_connections::connect::*;
}
mod connector {
    pub(crate) use intent_node_connections::connector::*;
}
mod final_stop {
    pub(crate) use intent_node_connections::final_stop::*;
}
mod forge_token;
mod git {
    pub(crate) use intent_node_connections::git::*;
}
mod home_calls;
mod home_move;
mod home_scripts;
mod home_state;
mod homes;
mod inbound;
mod launch;
mod lease {
    pub(crate) use intent_node_connections::lease::*;
}
mod link {
    pub(crate) use intent_node_connections::link::*;
}
mod machine {
    pub(crate) use intent_node_connections::machine::*;
}
mod methods;
pub mod order;
mod quick_action;
mod rpc;
mod setup_retry;
mod terminals;
mod view {
    pub(crate) use intent_node_connections::view::*;
}

pub use intent_node_connections::connector::NodeStatus;
pub use intent_node_connections::runtime::RemoteAgentRuntime;
pub use intent_node_connections::MachineConnection;
pub use launch::RemoteLaunch;

/// One machine's connection, as the domain holds it.
pub type Node = Arc<dyn MachineConnection>;

#[cfg(test)]
mod tests;

/// The remote feature's state: one connector per exposed machine, started
/// when the machine is first needed.
#[derive(Default)]
pub struct RemoteFeature {
    /// The machines' connectors and lifecycle locks.
    connections: Connections,
    tailcat_bin: Mutex<Option<PathBuf>>,
    /// Connector timing; the defaults unless a test shortens them.
    tuning: Mutex<Option<connector::Tuning>>,
    /// E2B cloud machines: starts in flight and the settings lock.
    pub(crate) cloud: cloud::CloudState,
    /// The GitHub token every linked machine holds (nodeProtocol 15).
    forge: std::sync::OnceLock<Arc<link::forge_token::ForgeTokens>>,
    /// Where remote agents' and homes' refs live.
    git: GitStore,
    /// The `hello.headSession` every link of this backend offers.
    head_session: HeadSession,
    /// Taken by every change of the ordered machine list
    /// ([`order`]): one per backend, so two tenants never wait on each
    /// other.
    pub(crate) order_lock: tokio::sync::Mutex<()>,
    /// Set by [`RemoteFeature::close_links`]: no connector starts again,
    /// and the feature's background work (the cloud supervisor, home-move
    /// runners) ends.
    closed: Closed,
    /// The feature's background work in flight ([`spawn_until_closed`]).
    background: Background,
    /// The workspaces whose home move runs on this backend.
    pub(crate) home_moves: home_move::Running,
}

/// [`RemoteFeature::background_tasks`]: how many tasks
/// [`spawn_until_closed`] started that have not ended.
#[derive(Default)]
struct Background {
    running: std::sync::atomic::AtomicUsize,
    idle: tokio::sync::Notify,
}

/// Held by one background task; counts it out when the task ends.
pub(crate) struct BackgroundTask<'a>(&'a Background);

impl<'a> BackgroundTask<'a> {
    /// Count a background task of `feature` in, until this is dropped.
    pub(crate) fn of(feature: &'a RemoteFeature) -> Self {
        Self::start(&feature.background)
    }

    fn start(background: &'a Background) -> Self {
        background
            .running
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Self(background)
    }
}

impl Drop for BackgroundTask<'_> {
    fn drop(&mut self) {
        if self
            .0
            .running
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
            == 1
        {
            self.0.idle.notify_waiters();
        }
    }
}

/// Run `work` in the background for `svc`'s remote feature, until it ends or
/// the feature's links close ([`RemoteFeature::close_links`]): then it is
/// dropped where it stands, so it makes no further provider call or link
/// write. Once the links are closed, `work` never starts.
/// [`RemoteFeature::background_stopped`] waits until none is left.
pub(crate) fn spawn_until_closed<F>(svc: Services, work: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    // caller-binding: allow — the remote feature's background work, ended by close_links
    tokio::spawn(async move {
        let Some(feature) = svc.remote_feature() else {
            work.await;
            return;
        };
        let _task = BackgroundTask::start(&feature.background);
        if feature.links_closed() {
            return;
        }
        tokio::select! {
            () = feature.closed() => {}
            () = work => {}
        }
    });
}

/// [`RemoteFeature::closed`]: `true` once the links were closed.
struct Closed(tokio::sync::watch::Sender<bool>);

impl Default for Closed {
    fn default() -> Self {
        Self(tokio::sync::watch::channel(false).0)
    }
}

/// [`RemoteFeature::head_session`]: picked once per feature, so each start
/// of a backend (and each tenant of an orchestrator) has its own, and a
/// machine drops the agent secrets it held for the previous one
/// (nodeProtocol 14).
struct HeadSession(Arc<str>);

impl Default for HeadSession {
    fn default() -> Self {
        Self(uuid::Uuid::new_v4().to_string().into())
    }
}

/// [`RemoteFeature::connections`]: connectors in this process.
struct Connections(Arc<dyn NodeConnections>);

impl Default for Connections {
    fn default() -> Self {
        Self(Arc::new(InProcessNodeConnections::default()))
    }
}

/// [`RemoteFeature::git`]: the workspaces' own repositories unless set.
struct GitStore(Arc<dyn GitStorage>);

impl Default for GitStore {
    fn default() -> Self {
        Self(Arc::new(LocalGitStorage))
    }
}

/// The git storage of the remote feature registered on `svc`, or the
/// workspaces' own repositories without one.
pub(crate) fn git_storage(svc: &Services) -> Arc<dyn GitStorage> {
    svc.remote_feature()
        .map_or_else(|| GitStore::default().0, |feature| feature.git.0.clone())
}

impl RemoteFeature {
    /// The running connector to machine `machine_id`, if one was started.
    #[must_use]
    pub fn running_node(&self, machine_id: &str) -> Option<Node> {
        self.connections.0.running(machine_id)
    }

    /// Every running connector.
    #[must_use]
    pub fn running_nodes(&self) -> Vec<Node> {
        self.connections.0.all()
    }

    /// How many exposed machines this backend holds a live link to now.
    #[must_use]
    pub fn connected_machines(&self) -> usize {
        self.connections.0.connected_machines()
    }

    pub(crate) fn take_node(&self, machine_id: &str) -> Option<Node> {
        self.connections.0.take(machine_id)
    }

    /// Whether [`Self::close_links`] ran: no connector starts again.
    #[must_use]
    pub fn links_closed(&self) -> bool {
        *self.closed.0.borrow()
    }

    /// How many of the feature's background tasks are running.
    #[must_use]
    pub fn background_tasks(&self) -> usize {
        self.background
            .running
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Wait until none of the feature's background tasks is running: after
    /// [`Self::close_links`], every one has ended and no new one starts.
    /// Cancel-safe.
    pub async fn background_stopped(&self) {
        loop {
            let idle = self.background.idle.notified();
            if self.background_tasks() == 0 {
                return;
            }
            idle.await;
        }
    }

    /// Run `work` (a connector's dial-gate call: a sandbox resume, a tunnel
    /// revive) as part of the feature's background work: `None` without
    /// polling it once the links are closed, and `None` when they close
    /// while it runs, which drops it where it stands. Counted until it ends,
    /// so [`Self::background_stopped`] waits for it.
    pub(crate) async fn until_closed<F: std::future::Future>(&self, work: F) -> Option<F::Output> {
        let _task = BackgroundTask::of(self);
        if self.links_closed() {
            return None;
        }
        tokio::select! {
            () = self.closed() => None,
            output = work => Some(output),
        }
    }

    /// Wait until [`Self::close_links`] ran. Cancel-safe.
    pub(crate) async fn closed(&self) {
        let mut closed = self.closed.0.subscribe();
        // The sender lives in `self`, so the wait cannot end with an error.
        let _ = closed.wait_for(|closed| *closed).await;
    }

    /// Wait for machine `machine_id`'s lifecycle lock.
    pub(crate) async fn lifecycle(
        &self,
        machine_id: &str,
    ) -> intent_node_connections::LifecycleGuard {
        self.connections.0.lifecycle(machine_id).await
    }

    pub(crate) fn tailcat_bin(&self) -> Option<PathBuf> {
        lock(&self.tailcat_bin).clone()
    }

    pub(crate) fn tuning(&self) -> connector::Tuning {
        let mut tuning = lock(&self.tuning).clone().unwrap_or_default();
        tuning.head_session = Some(self.head_session.0.clone());
        tuning
    }

    /// The `hello.headSession` every link of this backend offers.
    #[must_use]
    pub fn head_session(&self) -> &str {
        &self.head_session.0
    }

    /// Shut down every running connector and close its link, as a fenced
    /// tenant must, and start no connector again: a start in flight is shut
    /// down instead of kept. Returns how many were shut down.
    pub fn close_links(&self) -> usize {
        self.closed.0.send_replace(true);
        let mut closed = 0;
        for node in self.connections.0.all() {
            if let Some(node) = self.connections.0.take(&node.machine_id()) {
                node.shutdown();
                closed += 1;
            }
        }
        closed
    }

    /// The GitHub token the machines hold, its watcher started.
    fn forge(&self, svc: &Services) -> Arc<link::forge_token::ForgeTokens> {
        let tuning = self.tuning();
        let forge = self
            .forge
            .get_or_init(|| {
                #[cfg(test)]
                {
                    let resolver = tuning.forge_resolver.0.clone().unwrap_or_else(|| {
                        Arc::new(|| -> intent_core::BoxFuture<'static, _> {
                            Box::pin(async { None })
                        })
                    });
                    Arc::new(link::forge_token::ForgeTokens::new(resolver))
                }
                #[cfg(not(test))]
                Arc::new(forge_token::for_services(svc))
            })
            .clone();
        forge_token::watch(&forge, svc, tuning.forge_refresh, tuning.forge_check);
        forge
    }

    /// The connector to exposed machine `machine_id`, started if needed
    /// under the machine's lifecycle lock, so a credential replacement or
    /// removal in progress finishes first and the connector is built from
    /// its outcome, not from the lease and token it replaces. Starting
    /// dials nothing.
    ///
    /// # Errors
    ///
    /// `not-found` for an unknown machine; when its token is missing from
    /// the secret store or its lease has ended.
    pub(crate) async fn node(&self, svc: &Services, machine_id: &str) -> Result<Node> {
        if !svc.node_mode() {
            if let Some(node) = self.running_node(machine_id) {
                return Ok(node);
            }
        }
        let _lifecycle = self.lifecycle(machine_id).await;
        self.node_locked(svc, machine_id).await
    }

    /// [`Self::node`] for a caller already holding `machine_id`'s lifecycle
    /// lock.
    ///
    /// # Errors
    ///
    /// As [`Self::node`].
    pub(crate) async fn node_locked(&self, svc: &Services, machine_id: &str) -> Result<Node> {
        // Node mode turns this daemon's orchestration off, including acting
        // as a backend: no outbound connector, so no remote agent can reach
        // its workspaces through forwarded calls. The inbound executor link
        // is separate and stays available.
        if svc.node_mode() {
            return Err(Error::NodeMode(
                "exposed machines are off: this daemon runs in node mode".into(),
            ));
        }
        if let Some(node) = self.running_node(machine_id) {
            return Ok(node);
        }
        if self.links_closed() {
            return Err(closed_links_error());
        }
        let row = svc
            .store()
            .get_exposed_machine(machine_id)
            .await?
            .ok_or_else(|| Error::NotFound(format!("exposed machine {machine_id}")))?;
        let token = svc
            .load_remote_machine_token(machine_id)
            .await?
            .ok_or_else(|| {
                Error::Internal(format!(
                    "the executor token of machine {machine_id} is missing; make the machine available again"
                ))
            })?;
        // A cloud machine dials through its sandbox's gate; a paused one
        // stays parked until something asks for its link.
        let gate = row.cloud.as_ref().map(|cloud| {
            if matches!(
                cloud.state,
                intent_core::machines::CloudSandboxState::Paused
                    | intent_core::machines::CloudSandboxState::Pausing
                    | intent_core::machines::CloudSandboxState::Resuming
            ) {
                self.cloud.park(machine_id);
            }
            cloud::CloudGate::new(svc.clone(), machine_id) as Arc<dyn link::gate::DialGate>
        });
        let target = machine::MachineTarget::new(&row, &token, self.tailcat_bin())?
            .tapped(svc.link_traffic().clone())
            .gated(gate);
        let node = self
            .connections
            .0
            .start(
                svc.store().clone(),
                target,
                self.tuning(),
                Some(inbound::ServicesInbound::shared(svc.clone())),
                Some(self.forge(svc)),
            )
            .await?;
        // A close that raced this start: it set the flag before this check
        // (so the node is shut down here) or found the node (so it shut it
        // down itself). Either way none outlives the close.
        if self.links_closed() {
            if let Some(started) = self.connections.0.take(machine_id) {
                started.shutdown();
            }
            node.shutdown();
            return Err(closed_links_error());
        }
        Ok(node)
    }
}

pub(crate) use intent_node_connections::lock;

fn closed_links_error() -> Error {
    Error::Internal("this backend's machine links are closed".into())
}

/// Register the remote feature and its remote-launch port on `services`.
/// Nothing dials a machine until one is needed.
#[must_use]
pub fn register(services: Services) -> Services {
    services
        .with_feature(Arc::new(RemoteFeature::default()))
        .with_remote_launch_port(Arc::new(RemoteLaunch))
}

/// Remote entry points the daemon calls on [`Services`].
/// See [`RemoteServicesExt::resume_exposed_machines`]. Starting a connector
/// dials nothing inline: each one connects in the background, with backoff
/// while it is unreachable, so a machine that is down never slows the start.
async fn resume_exposed_machines(svc: &Services) -> Vec<String> {
    let Some(feature) = svc.remote_feature() else {
        return Vec::new();
    };
    if svc.node_mode() {
        return Vec::new();
    }
    cloud::spawn_supervisor(svc);
    let candidates = match machines_with_live_records(svc).await {
        Ok(candidates) => candidates,
        Err(error) => {
            tracing::warn!(%error, "remote machines: could not read the recorded agents at start");
            return Vec::new();
        }
    };
    let mut started = Vec::new();
    for machine_id in candidates {
        match feature.node(svc, &machine_id).await {
            Ok(node) => {
                // A machine holding a home is dialed now (and kept), so its
                // home is backed up without waiting for other work there.
                let homes = svc
                    .store()
                    .homes_on_machine(&machine_id)
                    .await
                    .unwrap_or_default();
                // A machine that only owes a home stop is dialed too, so
                // the stop goes out.
                let owes_stops = svc
                    .store()
                    .list_home_stops(&machine_id)
                    .await
                    .is_ok_and(|stops| !stops.is_empty());
                if !homes.is_empty() || owes_stops {
                    node.view().set_holds_homes(true);
                    if !homes.is_empty() {
                        // A failing backup streak from before the restart
                        // still raises its warning on time.
                        home_state::resume_warnings(svc, node.view(), &homes).await;
                        // Its homes are detached if no link comes up within
                        // the link-loss grace.
                        node.home_link_lost();
                    }
                    // caller-binding: allow — dials a home's machine at start; the connector retries with backoff
                    tokio::spawn(async move {
                        if let Err(error) = node.connect().await {
                            tracing::info!(%error, "remote machine: a home's machine is not reachable yet");
                        }
                    });
                }
                started.push(machine_id);
            }
            Err(error) => {
                tracing::warn!(machine = %machine_id, %error, "remote machine: could not start its connector at start");
            }
        }
    }
    home_move::resume(svc).await;
    started
}

/// The exposed machines with a live lease that records agents that may
/// still run, or that hold a workspace's home.
async fn machines_with_live_records(svc: &Services) -> Result<Vec<String>> {
    let store = svc.store();
    let head_id = store.daemon_id().await?;
    let mut machines = Vec::new();
    for lease in store.list_owned_node_leases(&head_id).await? {
        let machine_id = &lease.owner.node_id;
        if lease.state.is_terminal() || machines.contains(machine_id) {
            continue;
        }
        // A paused cloud machine stays paused: its connector starts (and
        // resumes it) when something asks for it.
        match store.get_exposed_machine(machine_id).await? {
            None => continue,
            Some(row)
                if row.cloud.as_ref().is_some_and(|cloud| {
                    cloud.state != intent_core::machines::CloudSandboxState::Running
                }) =>
            {
                continue
            }
            Some(_) => {}
        }
        if connector::has_live_records(store, &lease.owner.lease_id).await?
            || homes::machine_holds_homes(store, machine_id).await?
        {
            machines.push(machine_id.clone());
        }
    }
    Ok(machines)
}

pub trait RemoteServicesExt {
    /// Give the feature the sitter-shipped tailcat binary, so machines can
    /// also be reached over their tailcat address. Without it only direct
    /// routes are tried.
    fn configure_remote_machines(&self, tailcat_bin: Option<PathBuf>);

    /// At daemon start: start the connector to every exposed machine that
    /// may still run this backend's agents, so it reconnects in the
    /// background and claims them. Returns the machines started.
    fn resume_exposed_machines(&self) -> impl std::future::Future<Output = Vec<String>> + Send;

    /// The remote feature, when registered.
    fn remote_feature(&self) -> Option<&RemoteFeature>;
}

impl RemoteServicesExt for Services {
    fn configure_remote_machines(&self, tailcat_bin: Option<PathBuf>) {
        if let Some(feature) = self.remote_feature() {
            *lock(&feature.tailcat_bin) = tailcat_bin;
        }
    }

    fn resume_exposed_machines(&self) -> impl std::future::Future<Output = Vec<String>> + Send {
        let svc = self.clone();
        async move { resume_exposed_machines(&svc).await }
    }

    fn remote_feature(&self) -> Option<&RemoteFeature> {
        self.feature::<RemoteFeature>()
    }
}
