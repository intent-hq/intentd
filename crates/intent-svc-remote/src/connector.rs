//! The head connector: dial the configured node, keep the link up, ingest
//! its journal and ack it.
//!
//! Every dial offers a generation durably advanced first, and the hello
//! carries head's ingested sequence, so the node replays exactly what head
//! has not ingested. Acks are batched: head persists the ingested sequence,
//! then sends it, at most every [`Tuning::ack_interval`] or
//! [`Tuning::ack_batch`] records, and right after the node's replay.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::Instant;

use intent_core::node_link::{
    JournalBody, LaunchCommand, LinkFrame, LinkOutcome, LinkRpcError, LinkWelcome,
};
use intent_core::nodes::NodeCounter;
use intent_core::placement::{AgentPlatform, PlatformAvailability};
use intent_core::remote_launch::{EffectiveIsolation, PlacementError};
use intent_core::{AgentId, Error, Result, WorkspaceId};
use intent_store::Store;
use intent_transport::node_link::{
    self as link, Backoff, DialedLink, IncomingRequest, LinkEvent, LinkEvents, NodeEndpoint,
    NodeLink,
};

use crate::agents::Agents;
use crate::config::HeadNodeConfig;
use crate::ingest::{Ingest, Step};
use crate::lease::HeadLease;
use crate::lock;
use crate::rpc::HeadCalls;
use crate::runtime::RemoteAgentRuntime;

/// JSON-RPC "method not found", for node requests head cannot serve.
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
    /// Wait after a permanent dial failure (certificate, token, ownership)
    /// before trying again, in case the operator fixed the node.
    pub permanent_retry: Duration,
    pub backoff: Backoff,
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
            #[cfg(test)]
            fail_ack_writes: Arc::default(),
        }
    }
}

/// What head knows about the configured node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeStatus {
    pub node_id: String,
    pub node_name: String,
    pub lease_id: String,
    /// Whether a link is up now.
    pub connected: bool,
    /// From the node's last welcome; `None` before the first bind.
    pub platform: Option<AgentPlatform>,
    pub max_agents: u32,
    /// The agents the node said it runs when the link was last bound.
    pub running_agents: Vec<AgentId>,
    pub intentd_version: Option<String>,
}

/// State shared by the connector task, the runtimes and [`RemoteNode`].
pub(crate) struct Shared {
    pub agents: Agents,
    pub tuning: Tuning,
    pub store: Store,
    /// Runtimes of agents the node was already running when head bound it
    /// (after a head restart), kept so their records keep flowing until the
    /// agent manager takes them.
    parked: Mutex<HashMap<AgentId, Arc<RemoteAgentRuntime>>>,
    link: watch::Sender<Option<NodeLink>>,
    status: Mutex<NodeStatus>,
    /// Agents whose stop has not been sent yet.
    pending_stops: Mutex<BTreeSet<AgentId>>,
    ingested: Mutex<NodeCounter>,
    shutdown: watch::Sender<bool>,
}

impl Shared {
    /// The current link, if one is up.
    pub(crate) fn current_link(&self) -> Option<NodeLink> {
        self.link.borrow().clone().filter(|link| !link.is_closed())
    }

    /// The current link, waiting up to `grace` for one to come up.
    pub(crate) async fn link_within(&self, grace: Duration) -> Option<NodeLink> {
        let mut rx = self.link.subscribe();
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

    /// Remember that `agent_id` must be stopped and send the stop now if a
    /// link is up; otherwise it goes out when the next link is bound.
    pub(crate) fn request_stop(
        self: &Arc<Self>,
        agent_id: &AgentId,
    ) -> Option<tokio::task::JoinHandle<()>> {
        lock(&self.pending_stops).insert(agent_id.clone());
        let runtime = tokio::runtime::Handle::try_current().ok()?;
        let shared = self.clone();
        Some(runtime.spawn(async move { shared.flush_stops().await }))
    }

    async fn flush_stops(&self) {
        let Some(link) = self.current_link() else {
            return;
        };
        let agents: Vec<AgentId> = lock(&self.pending_stops).iter().cloned().collect();
        for agent_id in agents {
            let frame = LinkFrame::Stop {
                agent_id: agent_id.clone(),
                stop_id: intent_core::node_link::StopId::new_v4(),
            };
            if link.send(&frame).await.is_err() {
                return;
            }
            lock(&self.pending_stops).remove(&agent_id);
        }
    }

    fn welcome(&self, welcome: &LinkWelcome) {
        let mut status = lock(&self.status);
        status.connected = true;
        status.platform = Some(welcome.capabilities.platform);
        status.max_agents = welcome.capabilities.capacity.agent_cap;
        status.running_agents.clone_from(&welcome.running_agents);
        status.intentd_version = Some(welcome.capabilities.intentd_version.clone());
    }

    fn disconnected(&self) {
        lock(&self.status).connected = false;
    }

    /// The node's platforms: supported once it has said what it runs, and
    /// available while it is connected with a free agent slot.
    pub(crate) fn availability(&self) -> PlatformAvailability {
        let status = lock(&self.status).clone();
        let supported: Vec<AgentPlatform> = status.platform.into_iter().collect();
        let free = status.connected
            && self.agents.live_count() < usize::try_from(status.max_agents).unwrap_or(usize::MAX);
        let available = if free { supported.clone() } else { Vec::new() };
        PlatformAvailability::new(supported, available, intent_core::now_iso())
    }
}

/// A running connector to the configured node. Clones share it.
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
    /// Register the node and its lease if needed, then start dialing.
    /// Forwarded `workspace_api` calls from the node's agents are answered
    /// with `calls`; without it, every node request is refused.
    pub(crate) async fn start(
        store: Store,
        config: HeadNodeConfig,
        calls: Option<HeadCalls>,
    ) -> Result<Self> {
        Self::start_with(store, config, Tuning::default(), calls).await
    }

    pub(crate) async fn start_with(
        store: Store,
        config: HeadNodeConfig,
        tuning: Tuning,
        calls: Option<HeadCalls>,
    ) -> Result<Self> {
        let lease = HeadLease::open(store, &config).await?;
        #[cfg(test)]
        let lease = lease.failing_ack_writes_when(tuning.fail_ack_writes.clone());
        let acked = lease.acked().await?;
        let store = lease.store().clone();
        let status = NodeStatus {
            node_id: config.node_id.clone(),
            node_name: config.name.clone(),
            lease_id: lease.owner().lease_id.clone(),
            connected: false,
            platform: None,
            max_agents: 0,
            running_agents: Vec::new(),
            intentd_version: None,
        };
        let shared = Arc::new(Shared {
            agents: Agents::default(),
            tuning,
            store,
            parked: Mutex::new(HashMap::new()),
            link: watch::channel(None).0,
            status: Mutex::new(status),
            pending_stops: Mutex::new(BTreeSet::new()),
            ingested: Mutex::new(acked),
            shutdown: watch::channel(false).0,
        });
        intent_core::spawn_daemon(run(
            shared.clone(),
            config.endpoint,
            lease,
            Ingest::new(acked),
            calls,
        ));
        Ok(Self { shared })
    }

    #[cfg(test)]
    pub(crate) fn shared(&self) -> Arc<Shared> {
        self.shared.clone()
    }

    /// What head knows about the node now.
    #[must_use]
    pub fn status(&self) -> NodeStatus {
        lock(&self.shared.status).clone()
    }

    /// The highest contiguous journal sequence head has ingested.
    #[must_use]
    pub fn ingested(&self) -> NodeCounter {
        *lock(&self.shared.ingested)
    }

    /// Attach `agent_id` (running, or about to be launched, on the node) and
    /// return its runtime. Records for the agent are delivered to the
    /// runtime from now on; attach before sending its launch.
    #[must_use]
    pub fn attach(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) -> Arc<RemoteAgentRuntime> {
        let sink = self.shared.agents.attach(agent_id, workspace_id.clone());
        Arc::new(RemoteAgentRuntime::new(
            agent_id.clone(),
            sink,
            self.shared.clone(),
        ))
    }

    /// Send a control frame (such as a launch) on the current link.
    ///
    /// # Errors
    ///
    /// `node-link-unavailable` when no link is up; the link's send error.
    pub async fn send(&self, frame: &LinkFrame) -> Result<()> {
        self.shared.send(frame).await
    }

    /// Agents head counts as running on the node.
    #[must_use]
    pub fn running(&self) -> usize {
        self.shared.agents.live_count()
    }

    /// The node's supported and currently available platforms.
    #[must_use]
    pub fn availability(&self) -> PlatformAvailability {
        self.shared.availability()
    }

    /// The runtime of `command`'s agent: the one kept for it when the node
    /// was already running it at bind, or a new one, attached before the
    /// launch is sent (waiting up to the link-loss grace for a link). A
    /// launch for an agent the node already runs is ignored by the node.
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
                    "{}: the execution node is not connected",
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

async fn run(
    shared: Arc<Shared>,
    endpoint: NodeEndpoint,
    lease: HeadLease,
    mut ingest: Ingest,
    calls: Option<HeadCalls>,
) {
    let mut backoff = shared.tuning.backoff.clone();
    let mut shutdown = shared.shutdown.subscribe();
    loop {
        let ingested = ingest.ingested();
        let dialed = link::dial_with_backoff(
            &endpoint,
            || {
                let lease = lease.clone();
                async move { lease.next_hello(ingested).await }
            },
            &mut backoff,
            shutdown.clone(),
        )
        .await;
        if *shutdown.borrow() {
            if let Ok(dialed) = dialed {
                dialed.link.close();
            }
            return;
        }
        let DialedLink {
            link,
            events,
            welcome,
        } = match dialed {
            Ok(dialed) => dialed,
            Err(error) => {
                tracing::warn!(
                    code = link::error_code(&error).unwrap_or("other"),
                    "remote node: cannot bind the node; retrying later"
                );
                tokio::select! {
                    () = tokio::time::sleep(shared.tuning.permanent_retry) => continue,
                    () = wait_true(&mut shutdown) => return,
                }
            }
        };
        if let Err(error) = lease.record_welcome(&welcome).await {
            tracing::warn!(%error, "remote node: could not record the node's welcome");
        }
        shared.welcome(&welcome);
        shared.link.send_replace(Some(link.clone()));
        tracing::info!(
            generation = link.binding().generation.0,
            ingested = ingested.0,
            "remote node: link bound"
        );
        claim(&shared, &welcome, &link).await;
        shared.flush_stops().await;
        let git = serve_git(&shared, &endpoint, &link).await;
        let served = Served {
            lease: &lease,
            link: &link,
            calls: calls.as_ref(),
        };
        serve(&shared, served, &mut ingest, events, &mut shutdown).await;
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
        if *shutdown.borrow() {
            return;
        }
        tracing::info!("remote node: link closed; redialing");
    }
}

/// What one bound link is served with.
struct Served<'a> {
    lease: &'a HeadLease,
    link: &'a NodeLink,
    calls: Option<&'a HeadCalls>,
}

/// Ingest one link's journal and answer its requests until it closes, a
/// live gap needs a rebind, or shutdown.
async fn serve(
    shared: &Arc<Shared>,
    Served { lease, link, calls }: Served<'_>,
    ingest: &mut Ingest,
    mut events: LinkEvents,
    shutdown: &mut watch::Receiver<bool>,
) {
    let mut acks = Acks::default();
    ingest.start_link();
    loop {
        let due = acks.due;
        tokio::select! {
            event = events.recv() => {
                let Some(event) = event else { break };
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
                                    "remote node: journal records were lost on the node"
                                );
                                record_placement(shared, &record).await;
                                shared.agents.deliver(record);
                            }
                            Step::Skip => {}
                            Step::Resync => {
                                tracing::warn!(
                                    seq = seq.0,
                                    ingested = ingest.ingested().0,
                                    "remote node: journal gap on the live link; rebinding"
                                );
                                break;
                            }
                        }
                        *lock(&shared.ingested) = ingest.ingested();
                        if acks.record(&shared.tuning) {
                            acks.flush(lease, link, ingest.ingested()).await;
                        }
                    }
                    LinkEvent::Frame(LinkFrame::ReplayComplete { through_seq }) => {
                        ingest.replay_complete();
                        tracing::debug!(through = through_seq.0, "remote node: replay complete");
                        acks.flush(lease, link, ingest.ingested()).await;
                    }
                    LinkEvent::Frame(frame) => {
                        tracing::debug!(channel = ?frame.channel(), "remote node: ignoring an unexpected frame");
                    }
                    LinkEvent::Request(request) => match calls {
                        Some(calls) => calls.answer(shared.clone(), request),
                        None => answer_unsupported(request),
                    },
                }
            }
            () = sleep_until(due), if due.is_some() => {
                acks.flush(lease, link, ingest.ingested()).await;
            }
            () = wait_true(shutdown) => break,
        }
    }
    // Persist what was ingested even if the ack can no longer be sent, so
    // the next hello asks for exactly the rest.
    acks.flush(lease, link, ingest.ingested()).await;
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
                    stop_id: intent_core::node_link::StopId::new_v4(),
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
