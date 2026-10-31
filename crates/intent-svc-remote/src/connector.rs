//! The connector to one exposed machine: connect on demand, keep the link
//! up while this backend has live agents there, ingest the machine's
//! journal and ack it, and disconnect once idle.
//!
//! A link is wanted while an agent attached here is live, a stop is waiting
//! to be sent, or someone asked to connect ([`RemoteNode::connect`], a
//! prompt waiting for the link). With none of those for
//! [`Tuning::idle_timeout`], the link is closed and nothing redials until
//! the machine is needed again. A link lost while it is wanted is redialed
//! with backoff.
//!
//! Every dial offers a generation durably advanced first, and the hello
//! carries head's ingested sequence, so the machine replays exactly what
//! head has not ingested. Acks are batched: head persists the ingested
//! sequence, then sends it, at most every [`Tuning::ack_interval`] or
//! [`Tuning::ack_batch`] records, and right after the machine's replay.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;

use intent_core::machines::RemoteMachineState;
use intent_core::node_link::{
    JournalBody, LaunchCommand, LinkFrame, LinkOutcome, LinkRpcError, LinkWelcome,
    MachineCapabilities,
};
use intent_core::nodes::NodeCounter;
use intent_core::placement::{AgentPlatform, PlatformAvailability};
use intent_core::remote_launch::{EffectiveIsolation, PlacementError};
use intent_core::{AgentId, Error, Result, WorkspaceId};
use intent_services::feature_api::Services;
use intent_store::Store;
use intent_transport::node_link::{
    self as link, Backoff, IncomingRequest, LinkEvent, LinkEvents, NodeEndpoint, NodeLink,
};

use crate::agents::Agents;
use crate::connect::{connect, ConnectFailure, ConnectTuning, Connected};
use crate::ingest::{Ingest, Step};
use crate::lease::HeadLease;
use crate::lock;
use crate::machine::MachineTarget;
use crate::rpc::HeadCalls;
use crate::runtime::RemoteAgentRuntime;

/// JSON-RPC "method not found", for machine requests head cannot serve.
const UNSUPPORTED_CODE: i32 = -32601;

/// Timing knobs; tests shorten them.
#[derive(Debug, Clone)]
pub(crate) struct Tuning {
    /// Longest an ingested record waits for its ack.
    pub ack_interval: Duration,
    /// Records ingested (or skipped) before an ack is sent at once.
    pub ack_batch: usize,
    /// How long a turn waits for a lost link to come back before failing.
    pub link_loss_grace: Duration,
    /// How often a waiting prompt checks the idle window and the link.
    pub turn_poll: Duration,
    /// Wait after a permanent dial failure (revoked credential, version
    /// mismatch) before trying again while agents are live.
    pub permanent_retry: Duration,
    pub backoff: Backoff,
    /// Close the link after this long without a reason to keep it.
    pub idle_timeout: Duration,
    /// How often a bound link checks whether it is still wanted.
    pub idle_check: Duration,
    /// Longest a capabilities request waits for the machine's reply.
    pub capabilities_timeout: Duration,
    pub connect: ConnectTuning,
    /// Test seam: fail every ack write while set.
    #[cfg(test)]
    pub fail_ack_writes: Arc<std::sync::atomic::AtomicBool>,
}

impl Default for Tuning {
    fn default() -> Self {
        Self {
            ack_interval: Duration::from_millis(100),
            ack_batch: 256,
            link_loss_grace: Duration::from_secs(60),
            turn_poll: Duration::from_secs(1),
            permanent_retry: Duration::from_secs(60),
            backoff: Backoff::default(),
            idle_timeout: Duration::from_secs(10 * 60),
            idle_check: Duration::from_secs(15),
            capabilities_timeout: Duration::from_secs(10),
            connect: ConnectTuning::default(),
            #[cfg(test)]
            fail_ack_writes: Arc::default(),
        }
    }
}

/// What head knows about the machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeStatus {
    pub node_id: String,
    pub node_name: String,
    pub lease_id: String,
    /// Whether a link is up now.
    pub connected: bool,
    /// From the machine's last welcome; `None` before the first bind.
    pub platform: Option<AgentPlatform>,
    pub max_agents: u32,
    /// The agents the machine said it runs when the link was last bound.
    pub running_agents: Vec<AgentId>,
    pub intentd_version: Option<String>,
}

/// A connect request; answered once a link is bound or the dial failed.
type Waiter = oneshot::Sender<std::result::Result<(), ConnectFailure>>;

/// State shared by the connector task, the runtimes and [`RemoteNode`].
pub(crate) struct Shared {
    pub agents: Agents,
    pub tuning: Tuning,
    pub store: Store,
    /// The backend's services: answer forwarded calls and publish
    /// `remoteMachine:changed`. `None` refuses every machine request.
    services: Option<Services>,
    /// Where the machine is dialed; refreshed from every welcome.
    target: Mutex<MachineTarget>,
    /// Runtimes of agents the machine was already running when head bound
    /// it (after a head restart), kept so their records keep flowing until
    /// the agent manager takes them.
    parked: Mutex<HashMap<AgentId, Arc<RemoteAgentRuntime>>>,
    link: watch::Sender<Option<NodeLink>>,
    status: Mutex<NodeStatus>,
    /// Agents whose stop has not been sent yet.
    pending_stops: Mutex<BTreeSet<AgentId>>,
    /// Waiting for the machine's next capabilities report.
    capability_waiters: Mutex<Vec<oneshot::Sender<MachineCapabilities>>>,
    ingested: Mutex<NodeCounter>,
    /// Asks the connector for a link: `None` just wakes it.
    demand: mpsc::UnboundedSender<Option<Waiter>>,
    shutdown: watch::Sender<bool>,
}

impl Shared {
    /// The current link, if one is up.
    pub(crate) fn current_link(&self) -> Option<NodeLink> {
        self.link.borrow().clone().filter(|link| !link.is_closed())
    }

    /// Ask the connector for a link without waiting.
    pub(crate) fn wake(&self) {
        let _ = self.demand.send(None);
    }

    /// The current link, asking for one and waiting up to `grace` for it.
    pub(crate) async fn link_within(&self, grace: Duration) -> Option<NodeLink> {
        let mut rx = self.link.subscribe();
        if self.current_link().is_none() {
            self.wake();
        }
        tokio::time::timeout(grace, async {
            loop {
                let current = rx.borrow_and_update().clone();
                if let Some(link) = current.filter(|link| !link.is_closed()) {
                    return Some(link);
                }
                if rx.changed().await.is_err() {
                    return None;
                }
            }
        })
        .await
        .ok()
        .flatten()
    }

    /// Send a control frame on the current link.
    pub(crate) async fn send(&self, frame: &LinkFrame) -> Result<()> {
        let link = self
            .current_link()
            .ok_or_else(|| Error::Internal(link::codes::UNAVAILABLE.into()))?;
        link.send(frame).await
    }

    /// Whether the link should stay up: a live agent, a stop to send or a
    /// capabilities request is waiting.
    fn wants_link(&self) -> bool {
        self.agents.live_count() > 0
            || !lock(&self.pending_stops).is_empty()
            || !lock(&self.capability_waiters).is_empty()
    }

    /// Remember that `agent_id` must be stopped and send the stop now if a
    /// link is up; otherwise it goes out when the next link is bound.
    pub(crate) fn request_stop(
        self: &Arc<Self>,
        agent_id: &AgentId,
    ) -> Option<tokio::task::JoinHandle<()>> {
        lock(&self.pending_stops).insert(agent_id.clone());
        let runtime = tokio::runtime::Handle::try_current().ok()?;
        let shared = self.clone();
        Some(runtime.spawn(async move {
            if shared.current_link().is_some() {
                shared.flush_stops().await;
            } else {
                shared.wake();
            }
        }))
    }

    /// Send the pending stops on the current link and confirm them; they
    /// leave the queue only once the machine has applied them.
    async fn flush_stops(&self) {
        let Some(link) = self.current_link() else {
            return;
        };
        let agents: Vec<AgentId> = lock(&self.pending_stops).iter().cloned().collect();
        if agents.is_empty() {
            return;
        }
        if let Err(error) = self.confirm_on(&link, &agents).await {
            tracing::debug!(%error, "remote machine: stops not confirmed yet; they stay queued");
        }
    }

    /// Send `agents`' stops on `link` and wait until the machine has
    /// applied them. Handing a frame to the link is not delivery; the
    /// machine handles its control frames in order, so its answer to a
    /// capabilities request sent after the stops on the same link confirms
    /// them. Confirmed stops leave the pending queue; on failure they stay.
    async fn confirm_on(&self, link: &NodeLink, agents: &[AgentId]) -> Result<()> {
        for agent_id in agents {
            link.send(&LinkFrame::Stop {
                agent_id: agent_id.clone(),
            })
            .await?;
        }
        let (tx, rx) = oneshot::channel();
        lock(&self.capability_waiters).push(tx);
        let answered = async {
            link.send(&LinkFrame::Capabilities).await?;
            tokio::time::timeout(self.tuning.capabilities_timeout, rx)
                .await
                .map_err(|_| {
                    Error::Internal(format!(
                        "{}: the machine did not confirm the stops",
                        link::codes::TIMEOUT
                    ))
                })?
                .map_err(|_| Error::Internal(link::codes::CLOSED.into()))
        }
        .await;
        lock(&self.capability_waiters).retain(|waiter| !waiter.is_closed());
        answered?;
        if link.is_closed() {
            return Err(Error::Internal(link::codes::CLOSED.into()));
        }
        let mut pending = lock(&self.pending_stops);
        for agent_id in agents {
            pending.remove(agent_id);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn has_pending_stops(&self) -> bool {
        !lock(&self.pending_stops).is_empty()
    }

    /// Dial `target` (the routes, port and pin a refreshed registration
    /// was accepted on) from now on.
    pub(crate) fn set_target(&self, target: MachineTarget) {
        *lock(&self.target) = target;
    }

    fn target(&self) -> MachineTarget {
        lock(&self.target).clone()
    }

    fn machine_id(&self) -> String {
        lock(&self.status).node_id.clone()
    }

    fn welcome(&self, welcome: &LinkWelcome) {
        let mut status = lock(&self.status);
        status.connected = true;
        status.running_agents.clone_from(&welcome.running_agents);
        drop(status);
        self.capabilities(&welcome.capabilities);
        if !welcome.hosts.is_empty() {
            let mut target = lock(&self.target);
            target.hosts.clone_from(&welcome.hosts);
            target.tc_address.clone_from(&welcome.tc_address);
        }
    }

    fn capabilities(&self, capabilities: &MachineCapabilities) {
        let mut status = lock(&self.status);
        status.platform = Some(capabilities.platform);
        status.max_agents = capabilities.capacity.agent_cap;
        status.intentd_version = Some(capabilities.intentd_version.clone());
    }

    fn disconnected(&self) {
        lock(&self.status).connected = false;
    }

    /// The machine's platforms: supported once it has said what it runs, and
    /// available while it is connected with a free agent slot.
    pub(crate) fn availability(&self) -> PlatformAvailability {
        let status = lock(&self.status).clone();
        let supported: Vec<AgentPlatform> = status.platform.into_iter().collect();
        let free = status.connected
            && self.agents.live_count() < usize::try_from(status.max_agents).unwrap_or(usize::MAX);
        let available = if free { supported.clone() } else { Vec::new() };
        PlatformAvailability::new(supported, available, intent_core::now_iso())
    }

    /// Persist what a new link reported: the machine's routes, capabilities
    /// and a healthy resting state.
    async fn record_link(&self, welcome: &LinkWelcome) {
        let id = self.machine_id();
        let now = intent_core::now_iso();
        let target = self.target();
        let recorded = async {
            self.store
                .record_exposed_machine_connection(
                    &id,
                    &target.hosts,
                    target.tc_address.as_deref(),
                    &now,
                )
                .await?;
            self.store
                .record_exposed_machine_capabilities(&id, &welcome.capabilities)
                .await?;
            self.store
                .set_exposed_machine_state(&id, RemoteMachineState::Disconnected)
                .await
        };
        if let Err(error) = recorded.await {
            tracing::warn!(%error, machine = %id, "remote machine: could not record the new link");
        }
    }

    /// Persist the resting state a failed connect leaves the machine in.
    async fn record_failure(&self, failure: &ConnectFailure) {
        let state = match failure {
            ConnectFailure::Unreachable(_) => RemoteMachineState::Unreachable,
            ConnectFailure::Revoked(_) => RemoteMachineState::Revoked,
            ConnectFailure::UpdateRequired(_) => RemoteMachineState::UpdateRequired,
            ConnectFailure::Local(_) => return,
        };
        let id = self.machine_id();
        if let Err(error) = self.store.set_exposed_machine_state(&id, state).await {
            tracing::warn!(%error, machine = %id, "remote machine: could not record its state");
        }
    }

    /// Publish `remoteMachine:changed` with the machine as it is now.
    pub(crate) async fn announce(&self) {
        let Some(services) = &self.services else {
            return;
        };
        match self.store.get_exposed_machine(&self.machine_id()).await {
            Ok(Some(row)) => {
                let machine = crate::methods::machine_json(&row, Some(self));
                services
                    .publish_remote_machine_changed(serde_json::json!({ "machine": machine }))
                    .await;
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(%error, "remote machine: could not read it to announce a change");
            }
        }
    }

    /// Whether a link is up now.
    pub(crate) fn is_connected(&self) -> bool {
        lock(&self.status).connected
    }
}

/// A running connector to one exposed machine. Clones share it.
#[derive(Clone)]
pub struct RemoteNode {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for RemoteNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteNode")
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

impl RemoteNode {
    /// Start the connector to `target` on this backend's live lease there.
    /// Nothing is dialed until a link is wanted. Forwarded `workspace_api`
    /// calls are answered and changes announced with `services`; without
    /// them, every machine request is refused.
    pub(crate) async fn start(
        store: Store,
        target: MachineTarget,
        tuning: Tuning,
        services: Option<Services>,
    ) -> Result<Self> {
        let lease = HeadLease::open(store, &target.id).await?;
        #[cfg(test)]
        let lease = lease.failing_ack_writes_when(tuning.fail_ack_writes.clone());
        let acked = lease.acked().await?;
        let store = lease.store().clone();
        let status = NodeStatus {
            node_id: target.id.clone(),
            node_name: target.name.clone(),
            lease_id: lease.owner().lease_id.clone(),
            connected: false,
            platform: None,
            max_agents: 0,
            running_agents: Vec::new(),
            intentd_version: None,
        };
        let (demand, demands) = mpsc::unbounded_channel();
        let shared = Arc::new(Shared {
            agents: Agents::default(),
            tuning,
            store,
            services,
            target: Mutex::new(target),
            parked: Mutex::new(HashMap::new()),
            link: watch::channel(None).0,
            status: Mutex::new(status),
            pending_stops: Mutex::new(BTreeSet::new()),
            capability_waiters: Mutex::new(Vec::new()),
            ingested: Mutex::new(acked),
            demand,
            shutdown: watch::channel(false).0,
        });
        // After a backend restart, agents placed here may still run: bind
        // once to claim them (and let the idle timer close an unneeded link).
        let lease_id = lease.owner().lease_id.clone();
        if !shared.store.list_remote_agents(&lease_id).await?.is_empty() {
            shared.wake();
        }
        intent_core::spawn_daemon(run(shared.clone(), lease, Ingest::new(acked), demands));
        Ok(Self { shared })
    }

    #[cfg(test)]
    pub(crate) fn shared(&self) -> Arc<Shared> {
        self.shared.clone()
    }

    /// What head knows about the machine now.
    #[must_use]
    pub fn status(&self) -> NodeStatus {
        lock(&self.shared.status).clone()
    }

    /// The highest contiguous journal sequence head has ingested.
    #[must_use]
    pub fn ingested(&self) -> NodeCounter {
        *lock(&self.shared.ingested)
    }

    /// Attach `agent_id` (running, or about to be launched, on the machine)
    /// and return its runtime. Records for the agent are delivered to the
    /// runtime from now on; attach before sending its launch. A live agent
    /// keeps the link up.
    #[must_use]
    pub fn attach(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) -> Arc<RemoteAgentRuntime> {
        let sink = self.shared.agents.attach(agent_id, workspace_id.clone());
        self.shared.wake();
        Arc::new(RemoteAgentRuntime::new(
            agent_id.clone(),
            sink,
            self.shared.clone(),
        ))
    }

    /// Connect now if no link is up, and wait for the bind.
    ///
    /// # Errors
    ///
    /// `machine-unreachable` (with the routes tried), `machine-revoked` or
    /// `machine-update-required`.
    pub async fn connect(&self) -> Result<()> {
        if self.shared.current_link().is_some() {
            // Keeps the idle timer from closing the link under the caller.
            self.shared.wake();
            return Ok(());
        }
        self.connect_raw()
            .await
            .map_err(|failure| failure.into_error(&self.status().node_name))
    }

    async fn connect_raw(&self) -> std::result::Result<(), ConnectFailure> {
        let (tx, rx) = oneshot::channel();
        if self.shared.demand.send(Some(tx)).is_err() {
            return Err(ConnectFailure::Local("the connector has stopped".into()));
        }
        let limit = self.shared.tuning.connect.total + Duration::from_secs(5);
        match tokio::time::timeout(limit, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ConnectFailure::Local("the connector has stopped".into())),
            Err(_) => Err(ConnectFailure::Unreachable(Vec::new())),
        }
    }

    /// Connect if needed and ask the machine for a fresh capabilities
    /// report; it is recorded and announced before this returns.
    ///
    /// # Errors
    ///
    /// As [`RemoteNode::connect`]; `node-link-timeout` when the machine
    /// does not answer.
    pub async fn refresh_capabilities(&self) -> Result<MachineCapabilities> {
        let (tx, rx) = oneshot::channel();
        lock(&self.shared.capability_waiters).push(tx);
        let reply = async {
            self.connect().await?;
            self.shared.send(&LinkFrame::Capabilities).await?;
            tokio::time::timeout(self.shared.tuning.capabilities_timeout, rx)
                .await
                .map_err(|_| {
                    Error::Internal(format!(
                        "{}: the machine did not report its capabilities",
                        link::codes::TIMEOUT
                    ))
                })?
                .map_err(|_| Error::Internal(link::codes::CLOSED.into()))
        }
        .await;
        // On failure, drop this request so it no longer holds the link.
        lock(&self.shared.capability_waiters).retain(|waiter| !waiter.is_closed());
        reply
    }

    /// Stop `agents` on the machine and wait until it has applied the stops.
    /// Handing a frame to the link is not delivery; the machine handles its
    /// control frames in order, so its answer to a capabilities request sent
    /// after the stops on the same link confirms them. Confirmed stops leave
    /// the pending queue; on failure they stay queued for the next link.
    ///
    /// # Errors
    ///
    /// The connect error; `node-link-unavailable` / `node-link-closed` when
    /// the link is lost first; `node-link-timeout` without an answer.
    pub(crate) async fn confirm_stops(&self, agents: &[AgentId]) -> Result<()> {
        self.connect().await?;
        let link = self
            .shared
            .current_link()
            .ok_or_else(|| Error::Internal(link::codes::UNAVAILABLE.into()))?;
        self.shared.confirm_on(&link, agents).await
    }

    /// Send a control frame (such as a launch) on the current link.
    ///
    /// # Errors
    ///
    /// `node-link-unavailable` when no link is up; the link's send error.
    pub async fn send(&self, frame: &LinkFrame) -> Result<()> {
        self.shared.send(frame).await
    }

    /// Agents head counts as running on the machine.
    #[must_use]
    pub fn running(&self) -> usize {
        self.shared.agents.live_count()
    }

    /// Stop every agent attached here: their runtimes end and the machine
    /// is told to stop them. Returns every agent with a stop not yet
    /// confirmed by the machine, these included.
    #[must_use = "the stops are only confirmed once delivered"]
    pub fn stop_all(&self) -> Vec<AgentId> {
        let mut stopped = self.shared.agents.live_ids();
        for agent_id in &stopped {
            self.shared.agents.close(agent_id);
            let _ = self.shared.request_stop(agent_id);
        }
        for agent_id in lock(&self.shared.pending_stops).iter() {
            if !stopped.contains(agent_id) {
                stopped.push(agent_id.clone());
            }
        }
        stopped
    }

    /// The machine's supported and currently available platforms.
    #[must_use]
    pub fn availability(&self) -> PlatformAvailability {
        self.shared.availability()
    }

    pub(crate) fn shared_ref(&self) -> &Shared {
        &self.shared
    }

    /// The runtime of `command`'s agent: the one kept for it when the
    /// machine was already running it at bind, or a new one, attached before
    /// the launch is sent (waiting up to the link-loss grace for a link). A
    /// launch for an agent the machine already runs is ignored by it.
    ///
    /// # Errors
    ///
    /// `node-link-unavailable` when no link comes up within the grace.
    pub async fn launch(&self, command: LaunchCommand) -> Result<Arc<RemoteAgentRuntime>> {
        let parked = lock(&self.shared.parked).remove(&command.agent_id);
        if let Some(runtime) = parked.filter(|runtime| runtime.is_running()) {
            return Ok(runtime);
        }
        let runtime = self.attach(&command.agent_id, &command.workspace_id);
        let link = self
            .shared
            .link_within(self.shared.tuning.link_loss_grace)
            .await
            .ok_or_else(|| {
                Error::Internal(format!(
                    "{}: the machine is not connected",
                    link::codes::UNAVAILABLE
                ))
            })?;
        link.send(&LinkFrame::Launch(command)).await?;
        Ok(runtime)
    }

    /// Stop dialing and close the link.
    pub fn shutdown(&self) {
        self.shared.shutdown.send_replace(true);
        if let Some(link) = self.shared.link.borrow().as_ref() {
            link.close();
        }
    }
}

/// Why a bound link stopped being served.
enum Ended {
    /// Nothing wanted it for the idle timeout.
    Idle,
    /// It closed, or a live gap needs a rebind.
    Lost,
    Shutdown,
}

async fn run(
    shared: Arc<Shared>,
    lease: HeadLease,
    mut ingest: Ingest,
    mut demands: mpsc::UnboundedReceiver<Option<Waiter>>,
) {
    let calls = shared.services.clone().map(HeadCalls::new);
    let mut backoff = shared.tuning.backoff.clone();
    let mut shutdown = shared.shutdown.subscribe();
    let mut waiters: Vec<Waiter> = Vec::new();
    // Redial after a failure or a lost link while the link is wanted.
    let mut retry = false;
    loop {
        if !retry && waiters.is_empty() && !shared.wants_link() {
            tokio::select! {
                demand = demands.recv() => match demand {
                    Some(waiter) => waiters.extend(waiter),
                    None => return,
                },
                () = wait_true(&mut shutdown) => return,
            }
        }
        while let Ok(waiter) = demands.try_recv() {
            waiters.extend(waiter);
        }
        let target = shared.target();
        let version = lock(&shared.status).intentd_version.clone();
        let ingested = ingest.ingested();
        let connected = connect(&target, &shared.tuning.connect, version.as_deref(), || {
            let lease = lease.clone();
            async move { lease.next_hello(ingested).await }
        })
        .await;
        if *shutdown.borrow() {
            if let Ok(connected) = connected {
                connected.dialed.link.close();
            }
            return;
        }
        let Connected { dialed, endpoint } = match connected {
            Ok(connected) => connected,
            Err(failure) => {
                tracing::warn!(machine = %target.id, failure = ?failure, "remote machine: cannot connect");
                shared.record_failure(&failure).await;
                shared.announce().await;
                for waiter in waiters.drain(..) {
                    let _ = waiter.send(Err(failure.clone()));
                }
                // Fail the capability requests; they no longer hold a link.
                lock(&shared.capability_waiters).clear();
                retry = shared.wants_link();
                if retry {
                    let delay = match failure {
                        ConnectFailure::Revoked(_) | ConnectFailure::UpdateRequired(_) => {
                            shared.tuning.permanent_retry
                        }
                        _ => backoff.next_delay(),
                    };
                    tokio::select! {
                        () = tokio::time::sleep(delay) => {}
                        demand = demands.recv() => match demand {
                            Some(waiter) => waiters.extend(waiter),
                            None => return,
                        },
                        () = wait_true(&mut shutdown) => return,
                    }
                }
                continue;
            }
        };
        let link::DialedLink {
            link,
            events,
            welcome,
        } = dialed;
        backoff.reset();
        retry = false;
        if let Err(error) = lease.record_welcome(&welcome).await {
            tracing::warn!(%error, "remote machine: could not record the welcome");
        }
        shared.welcome(&welcome);
        shared.record_link(&welcome).await;
        shared.link.send_replace(Some(link.clone()));
        for waiter in waiters.drain(..) {
            let _ = waiter.send(Ok(()));
        }
        tracing::info!(
            machine = %target.id,
            generation = link.binding().generation.0,
            ingested = ingested.0,
            "remote machine: link bound"
        );
        shared.announce().await;
        claim(&shared, &welcome, &link).await;
        // Confirming needs the serve loop below to read the answer.
        let flushing = shared.clone();
        tokio::spawn(async move { flushing.flush_stops().await });
        let git = serve_git(&shared, &endpoint, &link).await;
        let served = Served {
            lease: &lease,
            link: &link,
            calls: calls.as_ref(),
        };
        let ended = serve(
            &shared,
            served,
            &mut ingest,
            events,
            &mut shutdown,
            &mut demands,
        )
        .await;
        if let Some(git) = git {
            git.abort();
        }
        link.close();
        shared.link.send_if_modified(|current| {
            let ours = current
                .as_ref()
                .is_some_and(|c| c.binding() == link.binding());
            if ours {
                *current = None;
            }
            ours
        });
        shared.disconnected();
        lock(&shared.capability_waiters).clear();
        match ended {
            Ended::Shutdown => return,
            Ended::Idle => {
                tracing::info!(machine = %target.id, "remote machine: idle; disconnected");
            }
            Ended::Lost => {
                retry = shared.wants_link();
                tracing::info!(machine = %target.id, redial = retry, "remote machine: link closed");
            }
        }
        shared.announce().await;
    }
}

/// What one bound link is served with.
struct Served<'a> {
    lease: &'a HeadLease,
    link: &'a NodeLink,
    calls: Option<&'a HeadCalls>,
}

/// Ingest one link's journal, answer its requests and connect requests,
/// until it closes, a live gap needs a rebind, it has been idle for
/// [`Tuning::idle_timeout`], or shutdown.
async fn serve(
    shared: &Arc<Shared>,
    Served { lease, link, calls }: Served<'_>,
    ingest: &mut Ingest,
    mut events: LinkEvents,
    shutdown: &mut watch::Receiver<bool>,
    demands: &mut mpsc::UnboundedReceiver<Option<Waiter>>,
) -> Ended {
    let mut acks = Acks::default();
    let mut idle_since: Option<Instant> = None;
    let mut idle_check = tokio::time::interval(shared.tuning.idle_check);
    idle_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ingest.start_link();
    let ended = loop {
        let due = acks.due;
        tokio::select! {
            event = events.recv() => {
                let Some(event) = event else { break Ended::Lost };
                match event {
                    LinkEvent::Frame(LinkFrame::Journal(record)) => {
                        let seq = record.seq;
                        match ingest.accept(seq) {
                            Step::Ingest => {
                                record_placement(shared, &record).await;
                                shared.agents.deliver(record);
                            }
                            Step::IngestAfterLoss { lost } => {
                                tracing::warn!(
                                    from = lost.0 .0,
                                    through = lost.1 .0,
                                    "remote machine: journal records were lost on the machine"
                                );
                                record_placement(shared, &record).await;
                                shared.agents.deliver(record);
                            }
                            Step::Skip => {}
                            Step::Resync => {
                                tracing::warn!(
                                    seq = seq.0,
                                    ingested = ingest.ingested().0,
                                    "remote machine: journal gap on the live link; rebinding"
                                );
                                break Ended::Lost;
                            }
                        }
                        *lock(&shared.ingested) = ingest.ingested();
                        if acks.record(&shared.tuning) {
                            acks.flush(lease, link, ingest.ingested()).await;
                        }
                    }
                    LinkEvent::Frame(LinkFrame::ReplayComplete { through_seq }) => {
                        ingest.replay_complete();
                        tracing::debug!(through = through_seq.0, "remote machine: replay complete");
                        acks.flush(lease, link, ingest.ingested()).await;
                    }
                    LinkEvent::Frame(LinkFrame::CapabilitiesReply { capabilities }) => {
                        shared.capabilities(&capabilities);
                        if let Err(error) = shared
                            .store
                            .record_exposed_machine_capabilities(&shared.machine_id(), &capabilities)
                            .await
                        {
                            tracing::warn!(%error, "remote machine: could not record its capabilities");
                        }
                        shared.announce().await;
                        let waiters: Vec<_> = lock(&shared.capability_waiters).drain(..).collect();
                        for waiter in waiters {
                            let _ = waiter.send(capabilities.clone());
                        }
                    }
                    LinkEvent::Frame(frame) => {
                        tracing::debug!(channel = ?frame.channel(), "remote machine: ignoring an unexpected frame");
                    }
                    LinkEvent::Request(request) => match calls {
                        Some(calls) => calls.answer(shared.clone(), request),
                        None => answer_unsupported(request),
                    },
                }
            }
            demand = demands.recv() => {
                // Someone needs the link: it is up, so answer at once and
                // restart the idle window.
                idle_since = None;
                if let Some(Some(waiter)) = demand {
                    let _ = waiter.send(Ok(()));
                }
            }
            _ = idle_check.tick() => {
                if shared.wants_link() {
                    idle_since = None;
                } else if idle_since.get_or_insert_with(Instant::now).elapsed()
                    >= shared.tuning.idle_timeout
                {
                    break Ended::Idle;
                }
            }
            () = sleep_until(due), if due.is_some() => {
                acks.flush(lease, link, ingest.ingested()).await;
            }
            () = wait_true(shutdown) => break Ended::Shutdown,
        }
    };
    // Persist what was ingested even if the ack can no longer be sent, so
    // the next hello asks for exactly the rest.
    acks.flush(lease, link, ingest.ingested()).await;
    ended
}

/// Ack batching for one link.
#[derive(Default)]
struct Acks {
    pending: usize,
    due: Option<Instant>,
    persisted: Option<NodeCounter>,
}

impl Acks {
    /// Count a received record; `true` when the batch is full.
    fn record(&mut self, tuning: &Tuning) -> bool {
        self.pending += 1;
        if self.due.is_none() {
            self.due = Some(Instant::now() + tuning.ack_interval);
        }
        self.pending >= tuning.ack_batch
    }

    /// Persist `seq`, then ack it. A failed persist sends no ack, so the
    /// node keeps the records.
    async fn flush(&mut self, lease: &HeadLease, link: &NodeLink, seq: NodeCounter) {
        self.pending = 0;
        self.due = None;
        if self.persisted != Some(seq) {
            match lease.persist_ack(seq).await {
                Ok(_) => self.persisted = Some(seq),
                Err(error) => {
                    tracing::warn!(%error, seq = seq.0, "remote node: could not persist the journal ack; not acking");
                    return;
                }
            }
        }
        if !link.is_closed() {
            let _ = link.send(&LinkFrame::Ack { seq }).await;
        }
    }
}

/// Claim the node's running agents at bind: an agent head has no record of
/// on this lease (a head crash mid-launch, a deleted agent) is stopped; one
/// head knows but has not attached since it started gets a runtime, kept
/// until the agent manager takes it, so its output is not dropped.
async fn claim(shared: &Arc<Shared>, welcome: &LinkWelcome, link: &NodeLink) {
    if welcome.running_agents.is_empty() {
        return;
    }
    let lease_id = lock(&shared.status).lease_id.clone();
    let claimed = match shared.store.list_remote_agents(&lease_id).await {
        Ok(claimed) => claimed,
        Err(error) => {
            // Without the records head cannot tell which agents are its
            // own; stopping them all could end live work, so wait for the
            // next bind.
            tracing::warn!(%error, "remote node: could not read this lease's agents; claiming nothing");
            return;
        }
    };
    let workspaces: HashMap<&AgentId, &WorkspaceId> = claimed
        .iter()
        .map(|record| (&record.agent_id, &record.workspace_id))
        .collect();
    for agent_id in &welcome.running_agents {
        match workspaces.get(agent_id) {
            None => {
                tracing::info!(agent = %agent_id, "remote node: stopping an agent head does not claim");
                let stop = LinkFrame::Stop {
                    agent_id: agent_id.clone(),
                };
                if link.send(&stop).await.is_err() {
                    return;
                }
            }
            Some(workspace_id) if !shared.agents.is_attached(agent_id) => {
                let sink = shared.agents.attach(agent_id, (*workspace_id).clone());
                let runtime = Arc::new(RemoteAgentRuntime::new(
                    agent_id.clone(),
                    sink,
                    shared.clone(),
                ));
                lock(&shared.parked).insert(agent_id.clone(), runtime);
            }
            Some(_) => {}
        }
    }
}

/// Dial the git connection for `link`'s binding and serve the node's git
/// sessions on it. A failure is logged: agents then cannot fetch their base,
/// and their launches fail with that error.
async fn serve_git(
    shared: &Arc<Shared>,
    endpoint: &NodeEndpoint,
    link: &NodeLink,
) -> Option<tokio::task::JoinHandle<()>> {
    match link::dial_git(endpoint, link.binding()).await {
        Ok(connection) => {
            let access = Arc::new(crate::git::HeadGitAccess::new(shared.clone()));
            // caller-binding: allow — serves node git sessions; access is checked per session against the store
            Some(tokio::spawn(link::serve_git(connection, access)))
        }
        Err(error) => {
            tracing::warn!(
                code = link::error_code(&error).unwrap_or("other"),
                "remote node: git connection not bound; remote agents cannot fetch or push"
            );
            None
        }
    }
}

/// Keep a remote agent's projected placement current: `isolated` once the
/// node reports its checkout and session ready, and the start error of an
/// agent that never got there.
async fn record_placement(shared: &Shared, record: &intent_core::node_link::JournalRecord) {
    let update: Box<dyn FnOnce(&mut intent_core::remote_launch::RemotePlacementView) + Send> =
        match &record.body {
            JournalBody::Started { .. } => Box::new(|view| {
                view.effective_isolation = EffectiveIsolation::Isolated;
                view.placement_error = None;
            }),
            JournalBody::Exited {
                error: Some(error), ..
            } => {
                let error = PlacementError::from_node_error(error);
                Box::new(move |view| {
                    if view.effective_isolation == EffectiveIsolation::Pending {
                        view.placement_error = Some(error);
                    }
                })
            }
            _ => return,
        };
    if let Err(error) = crate::view::update(
        &shared.store,
        &record.workspace_id,
        &record.agent_id,
        update,
    )
    .await
    {
        tracing::warn!(agent = %record.agent_id, %error, "remote node: could not record the agent's placement state");
    }
}

/// Without services to answer with, refuse a node request.
fn answer_unsupported(request: IncomingRequest) {
    // caller-binding: allow — only answers the link request; no service call
    tokio::spawn(async move {
        let outcome = LinkOutcome::Err(LinkRpcError {
            code: UNSUPPORTED_CODE,
            message: "remote-request-unsupported: head does not serve node requests yet".into(),
            data: None,
        });
        let _ = request.respond(outcome).await;
    });
}

async fn sleep_until(due: Option<Instant>) {
    match due {
        Some(due) => tokio::time::sleep_until(due).await,
        None => std::future::pending().await,
    }
}

async fn wait_true(rx: &mut watch::Receiver<bool>) {
    let _ = rx.wait_for(|flag| *flag).await;
}
