//! The head connector: dial the configured node, keep the link up, ingest
//! its journal and ack it.
//!
//! Every dial offers a generation durably advanced first, and the hello
//! carries head's ingested sequence, so the node replays exactly what head
//! has not ingested. Acks are batched: head persists the ingested sequence,
//! then sends it, at most every [`Tuning::ack_interval`] or
//! [`Tuning::ack_batch`] records, and right after the node's replay.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::Instant;

use intent_core::node_link::{LinkFrame, LinkOutcome, LinkRpcError, LinkWelcome};
use intent_core::nodes::NodeCounter;
use intent_core::placement::AgentPlatform;
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
use crate::runtime::RemoteAgentRuntime;

/// JSON-RPC "method not found", for node requests head does not serve yet.
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
        status.platform = Some(welcome.platform);
        status.max_agents = welcome.max_agents;
        status.running_agents.clone_from(&welcome.running_agents);
        status.intentd_version = Some(welcome.intentd_version.clone());
    }

    fn disconnected(&self) {
        lock(&self.status).connected = false;
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
    pub(crate) async fn start(store: Store, config: HeadNodeConfig) -> Result<Self> {
        Self::start_with(store, config, Tuning::default()).await
    }

    pub(crate) async fn start_with(
        store: Store,
        config: HeadNodeConfig,
        tuning: Tuning,
    ) -> Result<Self> {
        let lease = HeadLease::open(store, &config).await?;
        let acked = lease.acked().await?;
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
        ));
        Ok(Self { shared })
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

    /// Stop dialing and close the link.
    pub fn shutdown(&self) {
        self.shared.shutdown.send_replace(true);
        if let Some(link) = self.shared.link.borrow().as_ref() {
            link.close();
        }
    }
}

async fn run(shared: Arc<Shared>, endpoint: NodeEndpoint, lease: HeadLease, mut ingest: Ingest) {
    let mut backoff = shared.tuning.backoff.clone();
    let mut shutdown = shared.shutdown.subscribe();
    loop {
        let acked = ingest.ingested();
        let dialed = link::dial_with_backoff(
            &endpoint,
            || {
                let lease = lease.clone();
                async move { lease.next_hello(acked).await }
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
            acked = acked.0,
            "remote node: link bound"
        );
        shared.flush_stops().await;
        serve(&shared, &lease, &mut ingest, &link, events, &mut shutdown).await;
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

/// Ingest one link's journal until it closes, a live gap needs a rebind,
/// or shutdown.
async fn serve(
    shared: &Shared,
    lease: &HeadLease,
    ingest: &mut Ingest,
    link: &NodeLink,
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
                            Step::Ingest => shared.agents.deliver(record),
                            Step::IngestAfterLoss { lost } => {
                                tracing::warn!(
                                    from = lost.0 .0,
                                    through = lost.1 .0,
                                    "remote node: journal records were lost on the node"
                                );
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
                    LinkEvent::Request(request) => answer_unsupported(request),
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

/// Head serves no node requests yet: answer each with "unsupported".
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
