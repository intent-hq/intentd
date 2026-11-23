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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;

use crate::{LinkView, NodeInbound};
use intent_core::machine_calls::{MachineSecretAnswer, MachineSecretOp, MachineSkills};
use intent_core::machines::RemoteMachineState;
use intent_core::node_link::{
    JournalBody, LaunchCommand, LinkFrame, LinkId, LinkOutcome, LinkRpcError, LinkWelcome,
    MachineCapabilities, MachineSkill, PushWipRequest, PushWipResult, SecretForget, SecretList,
    SecretOutcome, SecretPut, SecretReply, SetupState, SkillsQuery, StopId, StopOutcome,
    FINAL_STOP_PROTOCOL, FORGE_FETCH_PROTOCOL, FORGE_TOKEN_PROTOCOL, SECRETS_PROTOCOL,
    SESSION_META_PROTOCOL, SKILLS_PROTOCOL,
};
use intent_core::nodes::NodeCounter;
use intent_core::placement::{AgentPlatform, PlatformAvailability};
use intent_core::remote_launch::{
    EffectiveIsolation, PlacementError, RemoteSetupState, RemoteSetupStatus,
};
use intent_core::{AgentId, Error, HomeId, Result, WorkspaceId};
use intent_node_link::{
    self as link, Backoff, IncomingRequest, LinkEvent, LinkEvents, NodeEndpoint, NodeLink,
};
use intent_store::Store;

use crate::agents::Agents;
use crate::connect::{connect, connect_after_resume, ConnectFailure, ConnectTuning, Connected};
use crate::ingest::{Acks, Ingest, Step};
use crate::lease::HeadLease;
use crate::lock;
use crate::machine::MachineTarget;
use crate::runtime::RemoteAgentRuntime;

/// Longest a bind waits to resolve the GitHub token afresh.
const FORGE_BIND_RESOLVE: Duration = Duration::from_secs(10);

/// Longest a launch that would carry `forge` waits for the bind's
/// `forgeTokenReply` (node-link.md § Forge token on the machine, rule 2).
const FORGE_LAUNCH_WAIT: Duration = Duration::from_secs(10);

/// Tunnel restarts a dial right after a resume may ask its gate for
/// ([`crate::link::gate::DialGate::revive`]) when nothing answers.
const MAX_REVIVES: u32 = 2;

/// JSON-RPC "method not found", for machine requests head cannot serve.
const UNSUPPORTED_CODE: i32 = -32601;

/// Timing knobs; tests shorten them.
#[derive(Debug, Clone)]
pub struct Tuning {
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
    /// Resend delays of an unacknowledged stop on a live link: 5 s, doubling
    /// to 60 s, ±20% jitter.
    pub stop_resend: Backoff,
    /// Longest `remoteMachines.remove` waits for its stops' acks, the
    /// connect included, on a link below nodeProtocol 9.
    pub remove_timeout: Duration,
    /// The same from nodeProtocol 9, where an ack waits for the machine's
    /// final push (bounded at 60 s there).
    pub final_remove_timeout: Duration,
    /// Longest a launch or prompt waits for an earlier stop of its agent to
    /// be acknowledged.
    pub stop_wait: Duration,
    /// How long an unacknowledged final stop's record is kept.
    pub final_stop_bound: Duration,
    /// Below nodeProtocol 9 a stop's acknowledgement does not wait for the
    /// exit push: the final-stop record (and the push it admits) is kept this
    /// much longer.
    pub legacy_push_grace: Duration,
    /// How often recorded final stops are checked against their bound,
    /// bound or not.
    pub final_stop_check: Duration,
    /// The clock final-stop bounds are measured against.
    pub clock: crate::final_stop::Clock,
    pub connect: ConnectTuning,
    /// How often linked machines' GitHub token is resolved again.
    pub forge_refresh: Duration,
    /// How often the token's watcher looks for a credential or gate change.
    pub forge_check: Duration,
    /// How often a linked machine's pending home stops are sent again
    /// (a `failed` ack, or none): every bind sends them too.
    pub home_stop_resend: Duration,
    /// The `hello.headSession` this backend's links offer: its remote
    /// feature's, so each tenant of an orchestrator has its own. Without one,
    /// the process's ([`crate::process_head_session`]).
    pub head_session: Option<Arc<str>>,
    /// Test seam: fail every ack write while set.
    #[cfg(any(test, feature = "test-support"))]
    pub fail_ack_writes: Arc<std::sync::atomic::AtomicBool>,
    /// Test seam: fail every claim's read of this lease's agent records
    /// while set.
    #[cfg(any(test, feature = "test-support"))]
    pub fail_claim_reads: Arc<std::sync::atomic::AtomicBool>,
    /// Test seam: runs at bind, after the queued stops are sent and before
    /// the link is published.
    #[cfg(any(test, feature = "test-support"))]
    pub before_publish: BeforePublish,
    /// Test seam: runs at bind, after the route is recorded and before the
    /// welcome's agents are claimed.
    #[cfg(any(test, feature = "test-support"))]
    pub before_claim: BeforePublish,
    /// Test seam: resolves the GitHub token the machines get; without one,
    /// a test backend hands out none (it never reads the host's).
    #[cfg(any(test, feature = "test-support"))]
    pub forge_resolver: ForgeResolver,
    /// Test seam: holds a below-nodeProtocol-9 grace task between dropping
    /// its record and leaving `Shared::in_grace`.
    #[cfg(any(test, feature = "test-support"))]
    pub grace_gate: GraceGate,
    /// Test seam: the link ownership the connector claims and fences with,
    /// instead of this process owning every link.
    #[cfg(any(test, feature = "test-support"))]
    pub ownership: OwnershipSeam,
}

/// See [`Tuning::ownership`].
#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Default)]
pub struct OwnershipSeam(pub Option<Arc<dyn crate::LinkOwnership>>);

#[cfg(any(test, feature = "test-support"))]
impl std::fmt::Debug for OwnershipSeam {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OwnershipSeam")
    }
}

/// See [`Tuning::forge_resolver`].
#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Default)]
pub struct ForgeResolver(pub Option<crate::link::forge_token::Resolver>);

#[cfg(any(test, feature = "test-support"))]
impl std::fmt::Debug for ForgeResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ForgeResolver")
    }
}

/// A test gate in a grace task: it notifies the first [`Notify`] on
/// reaching the gate, then waits for the second.
///
/// [`Notify`]: tokio::sync::Notify
#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Default, Debug)]
pub struct GraceGate(pub Option<Arc<(tokio::sync::Notify, tokio::sync::Notify)>>);

/// A test hook run with the connector's state just before a bound link is
/// published.
#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Default)]
pub struct BeforePublish(pub Option<PublishHook>);

/// See [`BeforePublish`].
#[cfg(any(test, feature = "test-support"))]
pub type PublishHook = Arc<dyn Fn(&Arc<Shared>) + Send + Sync>;

#[cfg(any(test, feature = "test-support"))]
impl std::fmt::Debug for BeforePublish {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.0.is_some() {
            "BeforePublish(set)"
        } else {
            "BeforePublish(none)"
        })
    }
}

/// How long a machine's link may stay down before a running turn fails
/// `node-link-unavailable` and its homes are detached: 60 s, or
/// `INTENTD_LINK_LOSS_GRACE_MS` (a test seam, so an e2e need not wait the
/// production grace).
fn link_loss_grace() -> Duration {
    std::env::var("INTENTD_LINK_LOSS_GRACE_MS")
        .ok()
        .and_then(|ms| ms.parse::<u64>().ok())
        .map_or(LINK_LOSS_GRACE, Duration::from_millis)
}

/// The production [`Tuning::link_loss_grace`].
const LINK_LOSS_GRACE: Duration = Duration::from_secs(60);

impl Default for Tuning {
    fn default() -> Self {
        Self {
            ack_interval: Duration::from_millis(100),
            ack_batch: 256,
            link_loss_grace: link_loss_grace(),
            turn_poll: Duration::from_secs(1),
            permanent_retry: Duration::from_secs(60),
            backoff: Backoff::default(),
            idle_timeout: Duration::from_secs(10 * 60),
            idle_check: Duration::from_secs(15),
            capabilities_timeout: Duration::from_secs(10),
            stop_resend: Backoff::new(Duration::from_secs(5), Duration::from_secs(60)),
            remove_timeout: Duration::from_secs(30),
            final_remove_timeout: Duration::from_secs(90),
            stop_wait: Duration::from_secs(30),
            final_stop_bound: intent_core::remote_launch::FINAL_STOP_BOUND,
            legacy_push_grace: Duration::from_secs(120),
            final_stop_check: Duration::from_secs(60 * 60),
            clock: crate::final_stop::Clock::default(),
            connect: ConnectTuning::default(),
            forge_refresh: Duration::from_secs(5 * 60),
            forge_check: Duration::from_secs(2),
            home_stop_resend: Duration::from_secs(2 * 60),
            head_session: None,
            #[cfg(any(test, feature = "test-support"))]
            fail_ack_writes: Arc::default(),
            #[cfg(any(test, feature = "test-support"))]
            fail_claim_reads: Arc::default(),
            #[cfg(any(test, feature = "test-support"))]
            before_publish: BeforePublish::default(),
            #[cfg(any(test, feature = "test-support"))]
            before_claim: BeforePublish::default(),
            #[cfg(any(test, feature = "test-support"))]
            forge_resolver: ForgeResolver::default(),
            #[cfg(any(test, feature = "test-support"))]
            grace_gate: GraceGate::default(),
            #[cfg(any(test, feature = "test-support"))]
            ownership: OwnershipSeam::default(),
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

/// Where a stopped agent's exit record is, while it matters to a relaunch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StopExit {
    /// The stop is not acknowledged and no exit record was ingested yet;
    /// `sent` once a stop frame for it went out (on any link).
    Pending { sent: bool },
    /// An exit record was ingested before the acknowledgement.
    Seen,
    /// Acknowledged as `stopped` with no exit record ingested yet: the
    /// record is on its way, and a relaunch must not attach before it.
    Owed,
}

/// One unacknowledged stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueuedStop {
    pub stop_id: StopId,
    /// The agent ends for good: the machine pushes its work one last time
    /// and removes its checkout (sent as final from nodeProtocol 9).
    pub final_stop: bool,
    /// Its record is in the store (`remote_final_stop`).
    pub durable: bool,
}

impl QueuedStop {
    fn plain(stop_id: StopId) -> Self {
        Self {
            stop_id,
            final_stop: false,
            durable: false,
        }
    }
}

/// A connect request; answered once a link is bound or the dial failed.
type Waiter = oneshot::Sender<std::result::Result<(), ConnectFailure>>;

/// State shared by the connector task, the runtimes and [`RemoteNode`].
pub struct Shared {
    pub agents: Agents,
    pub tuning: Tuning,
    pub store: Store,
    /// The head's domain: answers forwarded calls and hears the link's
    /// changes. `None` refuses every machine request, turns homes off,
    /// hands machines no forge token and keeps refs in the workspaces' own
    /// repositories.
    inbound: Option<Arc<dyn NodeInbound>>,
    /// Where the machine is dialed; refreshed from every welcome.
    target: Mutex<MachineTarget>,
    /// Runtimes of agents the machine was already running when head bound
    /// it (after a head restart), kept so their records keep flowing until
    /// the agent manager takes them.
    parked: Mutex<HashMap<AgentId, Arc<RemoteAgentRuntime>>>,
    link: watch::Sender<Option<NodeLink>>,
    status: Mutex<NodeStatus>,
    /// Agents whose stop the machine has not acknowledged yet, with the
    /// stop's id: one id per queued stop, reused by every resend on any
    /// link of this binding (nodeProtocol 7), and whether it is final.
    stops: Mutex<BTreeMap<AgentId, QueuedStop>>,
    /// Final stops acknowledged below nodeProtocol 9 whose records wait out
    /// [`Tuning::legacy_push_grace`]: never queued again.
    in_grace: Mutex<std::collections::HashSet<StopId>>,
    /// Final stops this process settled (acknowledged, or dropped past
    /// their bound), so each settles once: a later ack of the same stop, or
    /// a load that listed its record before it was dropped, settles nothing
    /// and queues nothing. Never pruned: one id per agent ended for good.
    settled_finals: Mutex<std::collections::HashSet<StopId>>,
    /// Agents that retired themselves during a turn, whose final stop
    /// waits for that turn to end: a bind leaves them running meanwhile.
    deferred_finals: Mutex<std::collections::HashSet<AgentId>>,
    /// This connector's own handle, for tasks that must not keep it alive.
    self_ref: Mutex<std::sync::Weak<Shared>>,
    /// The protocol the current (or last) link negotiated.
    protocol: std::sync::atomic::AtomicU32,
    /// Whether placement refuses this machine below nodeProtocol 10 for a
    /// workspace that needs a prepared checkout: this build's
    /// [`offers_prepared_checkout`](intent_core::node_link::offers_prepared_checkout),
    /// which tests turn on.
    prepared_checkout: std::sync::atomic::AtomicBool,
    /// Bumped whenever a stop leaves the queue (acknowledged, or dropped on
    /// revocation).
    stops_changed: watch::Sender<u64>,
    /// The stops a task is resending, with the link generation it resends
    /// on, so one link has one resender per stop.
    resending: Mutex<HashMap<StopId, u64>>,
    /// Where each stopped agent's exit record is, from its stop being
    /// queued until head has ingested that record (or none is coming).
    /// A relaunch waits out [`StopExit::Owed`], so the stopped process's
    /// exit never reaches the new runtime (intent-hq/intent#8457).
    stop_exits: Mutex<HashMap<AgentId, StopExit>>,
    /// Agents whose process ended when the machine started again from its
    /// disk: a `not-running` stop answer for one is final, no exit record
    /// is coming.
    lost_processes: Mutex<HashSet<AgentId>>,
    /// When the machine last had agent activity: a prompt sent, a journal
    /// record of an agent ingested, or a home call. A cloud machine's idle
    /// pause counts from it (intent-hq/intent#9237).
    last_activity: Mutex<Instant>,
    /// Uses of the machine over its link (home calls, terminal and home
    /// script opens, agent turns) hold the shared side for their whole
    /// time; an idle pause of a cloud machine holds the exclusive side
    /// from its home pushes until the link is closed.
    use_gate: Arc<tokio::sync::RwLock<()>>,
    /// Agents whose machine runs an acknowledged final stop settled.
    #[cfg(any(test, feature = "test-support"))]
    final_settles: Mutex<Vec<AgentId>>,
    /// Agents whose final stop was queued (and so began for their runs).
    #[cfg(any(test, feature = "test-support"))]
    final_begins: Mutex<Vec<AgentId>>,
    /// The label of the route the current (or last) link bound on.
    route: Mutex<Option<String>>,
    /// Set once a bind in this process has claimed the agents its welcome
    /// reported: from then on the attached agents are the machine's
    /// survivors. Set after the claim, not with the route, which a bind
    /// records before its claim.
    claimed: std::sync::atomic::AtomicBool,
    /// Serializes claims: a bind's and a retried one ([`RemoteNode::reclaim`]).
    claim_lock: tokio::sync::Mutex<()>,
    /// Set at start when agents that may still run are recorded on this
    /// lease: the link is wanted, and redialed with backoff, until a bind
    /// claims them.
    survivors_unclaimed: std::sync::atomic::AtomicBool,
    /// Waiting for the machine's next capabilities report.
    capability_waiters: Mutex<Vec<oneshot::Sender<MachineCapabilities>>>,
    /// Waiting for a `skillsReply` (nodeProtocol 12), by query id.
    skills_waiters: Mutex<HashMap<LinkId, oneshot::Sender<Vec<MachineSkill>>>>,
    /// Waiting for the machine's `pushWipReply`, by request id
    /// (nodeProtocol 13).
    push_waiters: Mutex<HashMap<intent_core::node_link::LinkId, oneshot::Sender<PushWipResult>>>,
    /// `checkoutRead`s waiting for their reply (nodeProtocol 16).
    checkout_waiters: Mutex<
        HashMap<
            intent_core::node_link::LinkId,
            oneshot::Sender<intent_core::node_link::CheckoutReadReply>,
        >,
    >,
    /// Waiting for the machine's `secretReply`, by request id
    /// (nodeProtocol 14).
    secret_waiters: Mutex<HashMap<LinkId, oneshot::Sender<SecretReply>>>,
    /// `oneShot`s waiting for their reply (nodeProtocol 18).
    one_shot_waiters: Mutex<HashMap<LinkId, oneshot::Sender<intent_core::node_link::OneShotReply>>>,
    /// `homeOpen`s and `homePush`es waiting for their reply (nodeProtocol
    /// 19).
    pub home_waiters: Mutex<HashMap<LinkId, oneshot::Sender<crate::link::homes::HomeReply>>>,
    /// `homeCall`s waiting for their reply (nodeProtocol 19).
    home_call_waiters:
        Mutex<HashMap<LinkId, oneshot::Sender<intent_core::node_link::HomeCallReply>>>,
    /// The machine holds a home of this backend's workspaces: its link stays
    /// up, so the home is backed up and reachable.
    pub holds_homes: std::sync::atomic::AtomicBool,
    /// Detaching this machine's homes after a link loss, and their
    /// backup warnings (nodes.md "Detached workspaces").
    pub home_link: crate::link::home_state::HomeLink,
    /// A worker resends this machine's pending home stops on a timer
    /// ([`crate::link::homes::ensure_stop_resends`]).
    pub home_stop_resends: std::sync::atomic::AtomicBool,
    /// Terminals on this machine's homes (nodeProtocol 19).
    pub terminals: crate::link::terminals::HomeTerminals,
    /// The home runs of saved scripts on this machine (nodeProtocol 19).
    pub home_scripts: crate::link::home_scripts::HomeScripts,
    /// Secret forgets for a machine that was not connected (nodeProtocol
    /// 14): sent after the next bind. Memory only; a backend restart drops
    /// them, and its new `headSession` drops the values on the machine.
    secret_forgets: Mutex<Vec<(AgentId, Option<String>)>>,
    /// One secret operation at a time per agent, the bind's flush of its
    /// queued forgets included, so a queued forget never lands after a
    /// newer `secretPut`.
    secret_locks: Mutex<HashMap<AgentId, Arc<tokio::sync::Mutex<()>>>>,
    /// The backend's GitHub token, sent after every bind and on each change
    /// (nodeProtocol 15); `None` hands out none.
    forge: Option<Arc<crate::link::forge_token::ForgeTokens>>,
    /// What the current link's machine confirmed of it.
    forge_confirmed: Mutex<crate::link::forge_token::Confirmed>,
    /// Bumped on each bind and each answer to a `forgeToken`, so a launch
    /// waiting on [`Self::forge_confirmed`] looks again.
    forge_answers: watch::Sender<u64>,
    ingested: Mutex<NodeCounter>,
    /// Each agent's checkout and setup output tail (nodeProtocol 10).
    setup_outputs: Mutex<HashMap<AgentId, SetupTail>>,
    /// The last refusal to make a cloud machine's sandbox dialable (a
    /// resume E2B refused), as the turn error a waiting prompt fails with;
    /// cleared by the next bind.
    cloud_refusal: watch::Sender<Option<String>>,
    /// Asks the connector for a link: `None` just wakes it.
    demand: mpsc::UnboundedSender<Option<Waiter>>,
    shutdown: watch::Sender<bool>,
}

impl Shared {
    /// The head's domain, when this connector has it.
    pub fn inbound(&self) -> Option<&Arc<dyn NodeInbound>> {
        self.inbound.as_ref()
    }

    /// The current link, if one is up.
    pub fn current_link(&self) -> Option<NodeLink> {
        self.link.borrow().clone().filter(|link| !link.is_closed())
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn is_parked(&self, agent_id: &AgentId) -> bool {
        lock(&self.parked).contains_key(agent_id)
    }

    /// Whether a launch for `agent_id` now reattaches to the run the
    /// machine kept across a restart rather than starting one (see
    /// [`RemoteNode::launch`]).
    pub fn reattaches(&self, agent_id: &AgentId) -> bool {
        lock(&self.parked)
            .get(agent_id)
            .is_some_and(|runtime| runtime.is_running())
            && !self.stop_queued(agent_id)
    }

    /// Whether the current link's machine confirmed holding the GitHub
    /// token (nodeProtocol 15).
    #[cfg(any(test, feature = "test-support"))]
    pub fn forge_token_confirmed(&self) -> bool {
        lock(&self.forge_confirmed).token()
    }

    /// Whether a launch may carry `forge` (node-link.md § Forge token on
    /// the machine, rules 2 and 8): the machine confirmed a token on this
    /// bind. Waits up to `within` for the bind's `forgeTokenReply`.
    async fn forge_ready(&self, within: Duration) -> bool {
        let mut answers = self.forge_answers.subscribe();
        let deadline = tokio::time::Instant::now() + within;
        loop {
            {
                let confirmed = lock(&self.forge_confirmed);
                if confirmed.settled() {
                    return confirmed.token();
                }
            }
            if !matches!(
                tokio::time::timeout_at(deadline, answers.changed()).await,
                Ok(Ok(()))
            ) {
                return lock(&self.forge_confirmed).token();
            }
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn tuning_ref(&self) -> &Tuning {
        &self.tuning
    }

    /// The protocol the current (or last) link negotiated; 0 before the
    /// first bind.
    pub fn protocol(&self) -> u32 {
        self.protocol.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// See the `prepared_checkout` field.
    pub fn offers_prepared_checkout(&self) -> bool {
        self.prepared_checkout
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(all(any(test, feature = "test-support"), unix))]
    pub fn offer_prepared_checkout_for_test(&self) {
        self.prepared_checkout
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn set_protocol_for_test(&self, protocol: u32) {
        self.protocol
            .store(protocol, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn weak(&self) -> std::sync::Weak<Shared> {
        lock(&self.self_ref).clone()
    }

    /// Ask the connector for a link without waiting.
    pub fn wake(&self) {
        let _ = self.demand.send(None);
    }

    /// The current link, asking for one and waiting up to `grace` for it.
    pub async fn link_within(&self, grace: Duration) -> Option<NodeLink> {
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
    ///
    /// # Errors
    ///
    /// When no link is up, or the send fails.
    pub async fn send(&self, frame: &LinkFrame) -> Result<()> {
        let link = self
            .current_link()
            .ok_or_else(|| Error::Internal(link::codes::UNAVAILABLE.into()))?;
        link.send(frame).await
    }

    /// Whether the link should stay up: a live agent, a stop to send or a
    /// capabilities request is waiting.
    fn wants_link(&self) -> bool {
        self.agents.live_count() > 0
            || self.machine_runs_live()
            || !lock(&self.stops).is_empty()
            || !lock(&self.capability_waiters).is_empty()
            || !lock(&self.skills_waiters).is_empty()
            || !lock(&self.push_waiters).is_empty()
            || !lock(&self.checkout_waiters).is_empty()
            || !lock(&self.one_shot_waiters).is_empty()
            || !lock(&self.secret_waiters).is_empty()
            || !lock(&self.home_waiters).is_empty()
            || !lock(&self.home_call_waiters).is_empty()
            || self.holds_homes.load(std::sync::atomic::Ordering::SeqCst)
            || !self.terminals.is_empty()
            || !self.home_scripts.is_empty()
            || (self
                .survivors_unclaimed
                .load(std::sync::atomic::Ordering::SeqCst)
                && !self.claimed.load(std::sync::atomic::Ordering::SeqCst))
    }

    /// Whether the machine has work now: a turn in flight, a live script
    /// run, a stop to deliver or a request waiting for its reply. A cloud
    /// machine without any is idle and may be paused.
    pub fn busy(&self) -> bool {
        self.agents.turns_in_flight() > 0
            || self.machine_runs_live()
            || !lock(&self.stops).is_empty()
            || !lock(&self.capability_waiters).is_empty()
            || !lock(&self.skills_waiters).is_empty()
            || !lock(&self.push_waiters).is_empty()
            || !lock(&self.checkout_waiters).is_empty()
            || !lock(&self.one_shot_waiters).is_empty()
            || !lock(&self.secret_waiters).is_empty()
            || !lock(&self.home_waiters).is_empty()
            || !lock(&self.home_call_waiters).is_empty()
            || !self.terminals.is_empty()
            || !self.home_scripts.is_empty()
    }

    /// When the machine last had agent activity (see `last_activity`).
    pub fn last_activity(&self) -> Instant {
        *lock(&self.last_activity)
    }

    /// Begin a use of the machine over its link; it waits while an idle
    /// pause runs, and the guard ends the use.
    #[must_use = "the use ends when the guard drops"]
    pub async fn begin_use(&self) -> tokio::sync::OwnedRwLockReadGuard<()> {
        self.use_gate.clone().read_owned().await
    }

    /// For an idle pause: wait up to `wait` for the uses in flight to end,
    /// and hold off new ones while the guard lives. `None` when uses kept
    /// it busy.
    pub async fn quiesce(&self, wait: Duration) -> Option<tokio::sync::OwnedRwLockWriteGuard<()>> {
        tokio::time::timeout(wait, self.use_gate.clone().write_owned())
            .await
            .ok()
    }

    /// Ask for a link: a parked cloud machine resumes for it.
    pub fn ask_link(&self) {
        let (tx, _rx) = oneshot::channel();
        let _ = self.demand.send(Some(tx));
    }

    /// The machine has agent activity now.
    pub fn note_activity(&self) {
        *lock(&self.last_activity) = Instant::now();
    }

    /// Whether something would keep a link up now (see the module docs).
    pub fn link_wanted(&self) -> bool {
        self.wants_link()
    }

    /// Close the current link, if any, without a redial: the cloud machine's
    /// sandbox is being paused, and its gate holds dials until a demand.
    pub fn close_link(&self) {
        if let Some(link) = self.current_link() {
            link.close();
        }
    }

    /// Fail waiting prompts with `turn_error` (see the `cloud_refusal`
    /// field).
    pub fn refuse_turns(&self, turn_error: &str) {
        self.cloud_refusal.send_replace(Some(turn_error.to_owned()));
    }

    /// Subscribe to cloud resume refusals (see the `cloud_refusal` field).
    pub fn cloud_refusals(&self) -> watch::Receiver<Option<String>> {
        self.cloud_refusal.subscribe()
    }

    /// Whether one of this machine's script runs is live: the machine stops
    /// a binding's scripts when its link closes, so the link stays up.
    fn machine_runs_live(&self) -> bool {
        self.inbound
            .as_ref()
            .is_some_and(|inbound| inbound.machine_runs_live(&self.machine_id()))
    }

    /// Lease end for `agents`' machine runs: send every stop, wait once
    /// until `deadline`; the agents whose script stop was unacknowledged.
    pub async fn end_agents_machine_runs(
        &self,
        agents: &[AgentId],
        deadline: Instant,
        reason: &str,
    ) -> Vec<AgentId> {
        match &self.inbound {
            Some(inbound) => {
                inbound
                    .end_agents_machine_runs(agents, deadline, reason)
                    .await
            }
            None => Vec::new(),
        }
    }

    /// Every live script run on this machine is lost with `reason`.
    pub async fn machine_runs_lost(&self, reason: &str) {
        if let Some(inbound) = &self.inbound {
            inbound.machine_runs_lost(&self.machine_id(), reason).await;
        }
    }

    /// Queue a stop for `agent_id` (reusing its id when one is already
    /// queued) and start resending it now if a link is up; otherwise it goes
    /// out right after the next bind. It stays queued, and is resent, until
    /// the machine acknowledges it; nothing waits for that here.
    pub fn request_stop(self: &Arc<Self>, agent_id: &AgentId) {
        let stop_id = self.queue_stop(agent_id);
        let Some(link) = self.current_link() else {
            self.wake();
            return;
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let shared = self.clone();
        let agent_id = agent_id.clone();
        // caller-binding: allow — resends one queued stop on its link until acknowledged
        runtime.spawn(async move { shared.resend(link, agent_id, stop_id).await });
    }

    /// The id of `agent_id`'s queued stop, queuing a new one if none is.
    /// A queued final stop stays final.
    fn queue_stop(&self, agent_id: &AgentId) -> StopId {
        self.watch_stop_exit(agent_id);
        lock(&self.stops)
            .entry(agent_id.clone())
            .or_insert_with(|| QueuedStop::plain(StopId::new_v4()))
            .stop_id
    }

    /// Queue final stop `stop_id` for `agent_id`, replacing a queued plain
    /// stop (or an older final stop). `durable` when its record is in the
    /// store, so the acknowledgement finishes that record.
    fn queue_final_stop(&self, agent_id: &AgentId, stop_id: StopId, durable: bool) {
        self.queue_final_stop_unless(agent_id, stop_id, durable, false);
    }

    /// [`Self::queue_final_stop`], except that with `unless_settled` a stop
    /// this process already settled is not queued again (decided under the
    /// queue's lock, like [`Self::claim_final_settle`]). Returns whether it
    /// was queued.
    fn queue_final_stop_unless(
        &self,
        agent_id: &AgentId,
        stop_id: StopId,
        durable: bool,
        unless_settled: bool,
    ) -> bool {
        let (replaced, settled) = {
            let mut stops = lock(&self.stops);
            let settled = lock(&self.settled_finals).contains(&stop_id);
            if unless_settled && settled {
                return false;
            }
            let replaced = stops.insert(
                agent_id.clone(),
                QueuedStop {
                    stop_id,
                    final_stop: true,
                    durable,
                },
            );
            (replaced, settled)
        };
        // Every queued final stop (requested, loaded at startup or on a
        // dial, or claimed at bind) begins for the agent's machine runs: the
        // machine stops its scripts itself, and none may start until the
        // stop is over. Except one this process already settled (a bind
        // sends it again to an agent the machine still runs): its runs are
        // settled, and its ack settles nothing, so nothing begins.
        if !settled {
            #[cfg(any(test, feature = "test-support"))]
            lock(&self.final_begins).push(agent_id.clone());
            if let Some(inbound) = &self.inbound {
                inbound.final_stop_begun(agent_id);
            }
        }
        self.watch_stop_exit(agent_id);
        if replaced.is_some_and(|old| old.stop_id != stop_id) {
            self.stops_changed.send_modify(|n| *n += 1);
        }
        true
    }

    /// End `agent_id` for good with `reason`: record its final stop when
    /// it is placed on this backend (falling back to one held in memory),
    /// detach its runtime and queue the stop. Returns the stop's id.
    pub async fn final_stop(
        self: &Arc<Self>,
        agent_id: &AgentId,
        reason: intent_core::remote_launch::FinalStopReason,
    ) -> StopId {
        let placed = match self.store.get_remote_agent(agent_id).await {
            Ok(placed) => placed,
            Err(error) => {
                tracing::warn!(agent = %agent_id, %error, "remote machine: could not read the agent's placement; stopping it from memory");
                None
            }
        };
        let recorded = match placed {
            Some(placed) => match crate::final_stop::record(&self.store, &placed, reason).await {
                Ok(stop_id) => Some(stop_id),
                Err(error) => {
                    tracing::warn!(agent = %agent_id, %error, "remote machine: could not record a final stop; stopping it from memory");
                    None
                }
            },
            None => None,
        };
        self.agents.close(agent_id);
        let (stop_id, durable) = match recorded {
            Some(stop_id) => (stop_id, true),
            None => (StopId::new_v4(), false),
        };
        self.request_final_stop(agent_id, stop_id, durable);
        stop_id
    }

    /// Queue final stop `stop_id` for `agent_id` and start resending it now
    /// if a link is up; otherwise it goes out right after the next bind.
    /// `durable` when its record is written.
    pub fn request_final_stop(
        self: &Arc<Self>,
        agent_id: &AgentId,
        stop_id: StopId,
        durable: bool,
    ) {
        // A request for a stop this process already settled (its record,
        // and so its id, outlives the ack below nodeProtocol 9 by the
        // grace) is moot: the machine acknowledged it.
        if !self.queue_final_stop_unless(agent_id, stop_id, durable, true) {
            return;
        }
        let Some(link) = self.current_link() else {
            self.wake();
            return;
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let shared = self.clone();
        let agent_id = agent_id.clone();
        // caller-binding: allow — resends one queued final stop on its link until acknowledged
        runtime.spawn(async move { shared.resend(link, agent_id, stop_id).await });
    }

    /// The queued stop `stop_id` of `agent_id`, if it still is.
    fn queued(&self, agent_id: &AgentId, stop_id: StopId) -> Option<QueuedStop> {
        lock(&self.stops)
            .get(agent_id)
            .copied()
            .filter(|queued| queued.stop_id == stop_id)
    }

    /// How long `remoteMachines.remove` waits for its stops' acks on the
    /// current link: longer from nodeProtocol 9, to fit the final push.
    pub fn remove_budget(&self) -> Duration {
        if self.speaks_final_stops() {
            self.tuning.final_remove_timeout
        } else {
            self.tuning.remove_timeout
        }
    }

    /// Whether the current link speaks final stops.
    fn speaks_final_stops(&self) -> bool {
        self.protocol.load(std::sync::atomic::Ordering::SeqCst) >= FINAL_STOP_PROTOCOL
    }

    /// Queue every final stop recorded on this lease, dropping (as done)
    /// those past their bound, whose checkout the machine's next bind sweep
    /// removes. A read failure is logged; the next bind tries again.
    pub async fn load_final_stops(&self) {
        let lease_id = lock(&self.status).lease_id.clone();
        let records = match self.store.list_final_stops(&lease_id).await {
            Ok(records) => records,
            Err(error) => {
                tracing::warn!(%error, "remote machine: could not read the pending final stops");
                return;
            }
        };
        self.settle_listed_final_stops(records).await;
    }

    /// Queue or drop (past their bound) the final stops `records` listed.
    /// Two loads can list the same record at once (the expiry timer and
    /// the dial loop): only the one whose `finish_final_stop` removed it
    /// settles the stop, so its machine runs settle once (intent#8610).
    async fn settle_listed_final_stops(
        &self,
        records: Vec<intent_core::remote_launch::RemoteFinalStopRecord>,
    ) {
        for record in records {
            let Ok(stop_id) = record.stop_id.parse::<StopId>() else {
                tracing::warn!(agent = %record.agent_id, "remote machine: a final stop record has a bad id; dropped");
                let _ = self
                    .store
                    .finish_final_stop(&record.agent_id, &record.stop_id)
                    .await;
                continue;
            };
            // Both read in one step under the queue's lock, which a legacy
            // ack's claim holds while it enters the grace: a stop is never
            // seen settled but not yet in its grace.
            let (in_grace, settled) = {
                let _stops = lock(&self.stops);
                let in_grace = lock(&self.in_grace).contains(&stop_id);
                (in_grace, lock(&self.settled_finals).contains(&stop_id))
            };
            if in_grace {
                continue;
            }
            if settled {
                // Settled already, by an ack whose drop of the record failed
                // or that this load listed before the drop: drop it now
                // (again), settling nothing.
                let _ = self
                    .store
                    .finish_final_stop(&record.agent_id, &record.stop_id)
                    .await;
                continue;
            }
            if crate::final_stop::expired(
                &record,
                self.tuning.final_stop_bound,
                self.tuning.clock.now(),
            ) {
                tracing::info!(agent = %record.agent_id, "remote machine: a final stop was not acknowledged within its bound; dropped");
                match self
                    .store
                    .finish_final_stop(&record.agent_id, &record.stop_id)
                    .await
                {
                    Ok(true) => {}
                    // Another load dropped it first and settled it.
                    Ok(false) => continue,
                    Err(error) => {
                        tracing::warn!(agent = %record.agent_id, %error, "remote machine: could not drop an expired final stop");
                        continue;
                    }
                }
                if !self.claim_final_settle(&record.agent_id, stop_id, true) {
                    // Its ack settled it while this load dropped the record.
                    continue;
                }
                // No ack settles this stop, so nothing tracks its exit.
                lock(&self.stop_exits).remove(&record.agent_id);
                self.final_stop_over(&record.agent_id).await;
                continue;
            }
            // Not if an ack settled it after this load listed its record.
            self.queue_final_stop_unless(&record.agent_id, stop_id, true, true);
        }
    }

    /// `agent_id`'s final stop is over (acknowledged, or dropped past its
    /// bound): any machine run the machine never reported ending is settled
    /// now, and its scripts are no longer refused as ending.
    async fn final_stop_over(&self, agent_id: &AgentId) {
        #[cfg(any(test, feature = "test-support"))]
        lock(&self.final_settles).push(agent_id.clone());
        // The agent is gone for good: so is its kept setup output.
        lock(&self.setup_outputs).remove(agent_id);
        if let Some(inbound) = &self.inbound {
            inbound
                .final_stop_settled(
                    agent_id,
                    "remote-script-lost: the agent was stopped for good",
                )
                .await;
        }
    }

    /// Whether `agent_id` has an unacknowledged stop.
    pub fn stop_queued(&self, agent_id: &AgentId) -> bool {
        lock(&self.stops).contains_key(agent_id)
    }

    /// Whether `agent_id` has an unacknowledged final stop.
    pub fn final_stop_queued(&self, agent_id: &AgentId) -> bool {
        lock(&self.stops)
            .get(agent_id)
            .is_some_and(|queued| queued.final_stop)
    }

    /// Stop `stop_id` of `agent_id` is acknowledged: it leaves the queue.
    /// An ack never confirms another stop of the same agent.
    fn confirm_stop(&self, agent_id: &AgentId, stop_id: StopId) {
        let mut stops = lock(&self.stops);
        if stops
            .get(agent_id)
            .is_some_and(|queued| queued.stop_id == stop_id)
        {
            stops.remove(agent_id);
            drop(stops);
            self.stops_changed.send_modify(|n| *n += 1);
        }
    }

    /// Claim the settling of final stop `stop_id` of `agent_id`: true for
    /// the first caller only (an ack, or an expiry that dropped its record),
    /// so the stop settles once however its acks, expiries and loads
    /// interleave. With `dequeue` (an expiry) the stop also leaves the
    /// queue in the same step, so an ack still in flight settles nothing;
    /// an ack leaves the queue itself once it is handled. Decided under the
    /// queue's lock, taken before [`Shared::settled_finals`] and
    /// [`Shared::in_grace`] (each taken alone under it).
    fn claim_final_settle(&self, agent_id: &AgentId, stop_id: StopId, dequeue: bool) -> bool {
        self.claim_final_settle_with(agent_id, stop_id, dequeue, false)
    }

    /// [`Self::claim_final_settle`]; with `grace` (an ack below
    /// nodeProtocol 9) the stop enters [`Shared::in_grace`] in the same
    /// step, so no load drops its record during the grace.
    fn claim_final_settle_with(
        &self,
        agent_id: &AgentId,
        stop_id: StopId,
        dequeue: bool,
        grace: bool,
    ) -> bool {
        let removed = {
            let mut stops = lock(&self.stops);
            if !lock(&self.settled_finals).insert(stop_id) {
                return false;
            }
            if grace {
                lock(&self.in_grace).insert(stop_id);
            }
            let queued = dequeue
                && stops
                    .get(agent_id)
                    .is_some_and(|queued| queued.stop_id == stop_id);
            if queued {
                stops.remove(agent_id);
            }
            queued
        };
        if removed {
            self.stops_changed.send_modify(|n| *n += 1);
        }
        true
    }

    /// The machine revoked this binding's credential, which stops its agents
    /// there: the queued stops are moot. Recorded final stops stay (a
    /// revocation alone drops no record) until acknowledged or past their
    /// bound.
    pub fn drop_stops(&self) {
        let dropped = {
            let mut stops = lock(&self.stops);
            let before = stops.len();
            stops.retain(|_, queued| queued.durable);
            // The machine journals no exit for a revoked binding's agents.
            lock(&self.stop_exits).retain(|agent_id, _| stops.contains_key(agent_id));
            stops.len() != before
        };
        if dropped {
            self.stops_changed.send_modify(|n| *n += 1);
        }
    }

    /// The queued stops, by agent.
    fn queued_stops(&self) -> Vec<(AgentId, QueuedStop)> {
        lock(&self.stops)
            .iter()
            .map(|(agent_id, queued)| (agent_id.clone(), *queued))
            .collect()
    }

    /// Send every queued stop on `link`, which nothing else uses yet, so
    /// they precede any launch or prompt on it.
    async fn send_queued(&self, link: &NodeLink) {
        let speaks_final = self.speaks_final_stops();
        for (agent_id, queued) in self.queued_stops() {
            self.stop_exit_sending(&agent_id);
            if link
                .send(&LinkFrame::Stop {
                    agent_id,
                    stop_id: queued.stop_id,
                    final_stop: queued.final_stop && speaks_final,
                })
                .await
                .is_err()
            {
                return;
            }
        }
    }

    /// Resend every queued stop on the just-published `link` until each is
    /// acknowledged. Taken after publication, the snapshot includes any stop
    /// queued while the bind was in progress (whose `request_stop` saw no
    /// link); a stop queued later starts its own resender. One link resends
    /// each stop once.
    fn resend_queued(self: &Arc<Self>, link: &NodeLink) {
        if link.protocol() >= SECRETS_PROTOCOL {
            let mut agents: Vec<AgentId> = lock(&self.secret_forgets)
                .iter()
                .map(|(agent_id, _)| agent_id.clone())
                .collect();
            agents.sort_by(|a, b| a.as_str().cmp(b.as_str()));
            agents.dedup();
            for agent_id in agents {
                let shared = self.clone();
                let link = link.clone();
                // caller-binding: allow — sends one agent's secret forgets queued while the machine was away
                tokio::spawn(async move {
                    let serial = shared.secret_lock(&agent_id);
                    let _serial = serial.lock().await;
                    shared.flush_secret_forgets(&link, &agent_id, None).await;
                });
            }
        }
        for (agent_id, queued) in self.queued_stops() {
            let stop_id = queued.stop_id;
            let shared = self.clone();
            let link = link.clone();
            // caller-binding: allow — resends one queued stop on its link until acknowledged
            tokio::spawn(async move { shared.resend(link, agent_id, stop_id).await });
        }
    }

    /// The lock that runs `agent_id`'s secret operations one at a time.
    fn secret_lock(&self, agent_id: &AgentId) -> Arc<tokio::sync::Mutex<()>> {
        lock(&self.secret_locks)
            .entry(agent_id.clone())
            .or_default()
            .clone()
    }

    /// Send `agent_id`'s queued secret forgets on `link`, in order, under
    /// its secret lock; those of the name `superseded` (a `secretPut` about
    /// to replace it) are dropped instead. A forget leaves the queue only
    /// once it is sent, so a cancelled flush loses none. Returns whether all
    /// were sent.
    async fn flush_secret_forgets(
        &self,
        link: &NodeLink,
        agent_id: &AgentId,
        superseded: Option<&str>,
    ) -> bool {
        if let Some(name) = superseded {
            lock(&self.secret_forgets)
                .retain(|(agent, forget)| agent != agent_id || forget.as_deref() != Some(name));
        }
        loop {
            let Some(name) = lock(&self.secret_forgets)
                .iter()
                .find(|(agent, _)| agent == agent_id)
                .map(|(_, name)| name.clone())
            else {
                return true;
            };
            let frame = LinkFrame::SecretForget(SecretForget {
                request_id: LinkId::new_v4(),
                agent_id: agent_id.clone(),
                name: name.clone(),
            });
            if let Err(error) = link.send(&frame).await {
                tracing::warn!(%error, "remote machine: a queued secret forget was not sent");
                return false;
            }
            let mut queued = lock(&self.secret_forgets);
            if let Some(at) = queued
                .iter()
                .position(|(agent, forget)| agent == agent_id && *forget == name)
            {
                queued.remove(at);
            }
        }
    }

    /// Send stop `stop_id` on `link` and resend it after 5 s, doubling to
    /// 60 s (±20%), until the machine acknowledges it or the link closes
    /// (the next bind sends it again). Only that stop's own ack confirms it.
    async fn resend(&self, link: NodeLink, agent_id: AgentId, stop_id: StopId) {
        let generation = link.binding().generation.0;
        {
            let mut resending = lock(&self.resending);
            if resending.get(&stop_id) == Some(&generation) {
                return;
            }
            resending.insert(stop_id, generation);
        }
        let mut delays = self.tuning.stop_resend.clone();
        while let Some(queued) = self
            .queued(&agent_id, stop_id)
            .filter(|_| !link.is_closed())
        {
            let final_stop = queued.final_stop && self.speaks_final_stops();
            let resent = self.stop_exit_sending(&agent_id);
            match link
                .stop_with(agent_id.clone(), stop_id, final_stop, delays.next_delay())
                .await
            {
                Ok(acked) => {
                    tracing::debug!(agent = %agent_id, outcome = ?acked.outcome, final_push = ?acked.final_push, "remote machine: stop acknowledged");
                    // A recorded stop settles only when this ack owns its
                    // record: an expiry may have dropped and settled it
                    // while the ack was in flight.
                    let settles = if queued.durable {
                        self.finish_final_stop(&agent_id, stop_id, final_stop).await
                    } else {
                        queued.final_stop && self.claim_final_settle(&agent_id, stop_id, false)
                    };
                    if settles {
                        // The agent is ended for good, even when a link below
                        // nodeProtocol 9 got the stop as a plain one.
                        self.final_stop_over(&agent_id).await;
                    }
                    // Only this stop's own ack settles its exit: one
                    // replaced meanwhile (by a final stop) leaves it alone.
                    if self.queued(&agent_id, stop_id).is_some() {
                        self.stop_exit_acked(&agent_id, acked.outcome, resent);
                    }
                    self.confirm_stop(&agent_id, stop_id);
                }
                Err(error) if link::error_code(&error) == Some(link::codes::TIMEOUT) => {}
                Err(_) => break,
            }
        }
        let mut resending = lock(&self.resending);
        if resending.get(&stop_id) == Some(&generation) {
            resending.remove(&stop_id);
        }
    }

    /// Final stop `stop_id` of `agent_id` is acknowledged: drop its record
    /// (and, unless the session can come back, the agent's placement). On a
    /// link below nodeProtocol 9 the acknowledgement does not wait for the
    /// exit push, so the record, and the push it admits, is kept for
    /// [`Tuning::legacy_push_grace`] first. Returns whether this ack
    /// settles the stop: it claimed the settling and (from nodeProtocol 9)
    /// dropped the record; not when an expiry or another ack settled it
    /// first, nor when the drop failed (a later load resends or expires it).
    async fn finish_final_stop(
        &self,
        agent_id: &AgentId,
        stop_id: StopId,
        sent_final: bool,
    ) -> bool {
        if !sent_final {
            // Acknowledged: not queued again, nor dropped as expired, while
            // its record waits out the grace.
            if !self.claim_final_settle_with(agent_id, stop_id, false, true) {
                return false;
            }
            let store = self.store.clone();
            let grace = self.tuning.legacy_push_grace;
            let agent_id = agent_id.clone();
            let shared = self.weak();
            #[cfg(any(test, feature = "test-support"))]
            let gate = self.tuning.grace_gate.0.clone();
            // caller-binding: allow — drops one acknowledged final stop's record after the push grace
            tokio::spawn(async move {
                tokio::time::sleep(grace).await;
                // Drop the record before leaving the grace: a load that
                // listed it earlier then finds it in the grace, or finds it
                // gone. Either way it is settled, so it settles nothing.
                if let Err(error) = store
                    .finish_final_stop(&agent_id, &stop_id.to_string())
                    .await
                {
                    tracing::warn!(agent = %agent_id, %error, "remote machine: could not drop an acknowledged final stop");
                }
                #[cfg(any(test, feature = "test-support"))]
                if let Some(gate) = gate {
                    gate.0.notify_one();
                    gate.1.notified().await;
                }
                if let Some(shared) = shared.upgrade() {
                    lock(&shared.in_grace).remove(&stop_id);
                }
            });
            return true;
        }
        match self
            .store
            .finish_final_stop(agent_id, &stop_id.to_string())
            .await
        {
            Ok(dropped) => dropped && self.claim_final_settle(agent_id, stop_id, false),
            Err(error) => {
                tracing::warn!(agent = %agent_id, %error, "remote machine: could not drop an acknowledged final stop; the next load resends or expires it");
                false
            }
        }
    }

    /// Wait until none of `agents` has an unacknowledged stop, or until
    /// `deadline`; returns those still unacknowledged.
    pub async fn wait_stops(&self, agents: &[AgentId], deadline: Instant) -> Vec<AgentId> {
        let mut changed = self.stops_changed.subscribe();
        loop {
            let unconfirmed: Vec<AgentId> = {
                let stops = lock(&self.stops);
                agents
                    .iter()
                    .filter(|agent_id| stops.contains_key(*agent_id))
                    .cloned()
                    .collect()
            };
            if unconfirmed.is_empty() {
                return unconfirmed;
            }
            match tokio::time::timeout_at(deadline, changed.changed()).await {
                Ok(Ok(())) => {}
                _ => return unconfirmed,
            }
        }
    }

    /// No launch or prompt goes out behind an unacknowledged stop of the
    /// same agent, so a resent stop can never stop a newer process. Waits up
    /// to [`Tuning::remove_timeout`].
    ///
    /// # Errors
    ///
    /// `node-link-unavailable` when the stop is still unacknowledged.
    pub async fn no_stop_pending(&self, agent_id: &AgentId) -> Result<()> {
        let deadline = Instant::now() + self.tuning.stop_wait;
        if self.stop_queued(agent_id) {
            // The stop needs a link: ask for one, since its own request may
            // have been spent on a dial that failed (a refused resume).
            self.wake();
        }
        if self.stop_queued(agent_id)
            && !self
                .wait_stops(std::slice::from_ref(agent_id), deadline)
                .await
                .is_empty()
        {
            return Err(Error::Internal(format!(
                "{}: the machine has not acknowledged this agent's earlier stop",
                link::codes::UNAVAILABLE
            )));
        }
        self.wait_stop_exit(agent_id, deadline).await;
        Ok(())
    }

    /// Track `agent_id`'s exit record from its stop being queued; an exit
    /// already owed stays owed.
    fn watch_stop_exit(&self, agent_id: &AgentId) {
        lock(&self.stop_exits)
            .entry(agent_id.clone())
            .or_insert(StopExit::Pending { sent: false });
    }

    /// A stop frame for `agent_id` is going out. Whether one went out
    /// before: if so, a `not-running` ack may answer an agent the earlier
    /// frame already stopped, whose exit record is still on its way.
    fn stop_exit_sending(&self, agent_id: &AgentId) -> bool {
        match lock(&self.stop_exits).get_mut(agent_id) {
            Some(StopExit::Pending { sent }) => std::mem::replace(sent, true),
            _ => false,
        }
    }

    /// The agent `record` reports exited, if it is an exit record.
    fn exited_agent(
        agent_id: &AgentId,
        record: &intent_core::node_link::JournalRecord,
    ) -> Option<AgentId> {
        matches!(record.body, JournalBody::Exited { .. }).then(|| agent_id.clone())
    }

    /// An exit record for `exited` was delivered: it settles its agent's
    /// stop, and one a relaunch waits for releases it. Called only after
    /// delivery, so a released relaunch never attaches before the old
    /// process's exit has reached the old runtime.
    fn stop_exit_ingested(&self, exited: Option<AgentId>) {
        let Some(agent_id) = exited else {
            return;
        };
        let mut exits = lock(&self.stop_exits);
        match exits.get(&agent_id) {
            Some(StopExit::Pending { .. }) => {
                exits.insert(agent_id, StopExit::Seen);
            }
            Some(StopExit::Owed) => {
                exits.remove(&agent_id);
                drop(exits);
                self.stops_changed.send_modify(|n| *n += 1);
            }
            Some(StopExit::Seen) | None => {}
        }
    }

    /// The machine acknowledged `agent_id`'s stop with `outcome`, answering
    /// a resend when `resent`. A `stopped` agent's exit record was
    /// journaled before the ack, but it may still be on its way: owed until
    /// ingested. A resend answered `not-running` may find the agent the
    /// earlier frame stopped still exiting (the machine answers
    /// `not-running` until its exit is journaled), so its record is owed
    /// too. A first stop answered `not-running` owes nothing.
    fn stop_exit_acked(&self, agent_id: &AgentId, outcome: StopOutcome, resent: bool) {
        let lost = lock(&self.lost_processes).remove(agent_id);
        let owed = outcome == StopOutcome::Stopped || (resent && !lost);
        let mut exits = lock(&self.stop_exits);
        if owed && matches!(exits.get(agent_id), Some(StopExit::Pending { .. })) {
            exits.insert(agent_id.clone(), StopExit::Owed);
        } else if exits.get(agent_id) != Some(&StopExit::Owed) {
            exits.remove(agent_id);
        }
    }

    /// The machine started again from its disk: `agents`' processes are
    /// gone, so no exit record of theirs is coming, and their runtimes here
    /// end with `error` (a waiting prompt is redriven on a relaunch).
    pub fn processes_lost(&self, agents: &[AgentId], error: &str) {
        lock(&self.lost_processes).extend(agents.iter().cloned());
        {
            let mut exits = lock(&self.stop_exits);
            for agent_id in agents {
                if exits.get(agent_id) == Some(&StopExit::Owed) {
                    exits.remove(agent_id);
                }
            }
        }
        self.stops_changed.send_modify(|n| *n += 1);
        self.agents.end_all(error);
    }

    /// A new process of `agent_id` is launched here: the mark of one lost
    /// in a disk resume no longer applies. While the lost process's stop
    /// is still out, its answer settles the mark instead: a resend answered
    /// `not-running` owes no exit record (intent-hq/intent#9238).
    pub fn launching(&self, agent_id: &AgentId) {
        if self.stop_queued(agent_id) || lock(&self.stop_exits).contains_key(agent_id) {
            return;
        }
        lock(&self.lost_processes).remove(agent_id);
    }

    /// Wait until `agent_id` owes no exit record, or until `deadline`.
    /// A record still owed then (lost on the machine, or its link is
    /// down) is given up on, so it never holds back a later relaunch.
    async fn wait_stop_exit(&self, agent_id: &AgentId, deadline: Instant) {
        let mut changed = self.stops_changed.subscribe();
        loop {
            if lock(&self.stop_exits).get(agent_id) != Some(&StopExit::Owed) {
                return;
            }
            match tokio::time::timeout_at(deadline, changed.changed()).await {
                Ok(Ok(())) => {}
                _ => break,
            }
        }
        let mut exits = lock(&self.stop_exits);
        if exits.get(agent_id) == Some(&StopExit::Owed) {
            exits.remove(agent_id);
            drop(exits);
            tracing::warn!(agent = %agent_id, "remote machine: the stopped agent's exit record did not arrive in time; relaunching anyway");
        }
    }

    /// The recorded final stops of this lease, listed now.
    ///
    /// # Panics
    ///
    /// When the store cannot list the stops.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn list_final_stops_for_test(
        &self,
    ) -> Vec<intent_core::remote_launch::RemoteFinalStopRecord> {
        let lease_id = lock(&self.status).lease_id.clone();
        self.store.list_final_stops(&lease_id).await.unwrap()
    }

    /// Settle `records` from two loads at once, as the expiry timer and the
    /// dial loop do when both list the same record.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn settle_twice_for_test(
        &self,
        records: Vec<intent_core::remote_launch::RemoteFinalStopRecord>,
    ) {
        tokio::join!(
            self.settle_listed_final_stops(records.clone()),
            self.settle_listed_final_stops(records),
        );
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn final_begins_for_test(&self) -> Vec<AgentId> {
        let mut agents = lock(&self.final_begins).clone();
        agents.dedup();
        agents
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn final_settles_for_test(&self) -> Vec<AgentId> {
        lock(&self.final_settles).clone()
    }

    /// How many times a final stop began (queued) for any agent.
    #[cfg(any(test, feature = "test-support"))]
    pub fn final_begin_count_for_test(&self) -> usize {
        lock(&self.final_begins).len()
    }

    /// Queue final stop `stop_id` of `agent_id` as recorded, as a bind's
    /// claim does.
    #[cfg(any(test, feature = "test-support"))]
    pub fn queue_final_stop_for_test(&self, agent_id: &AgentId, stop_id: StopId) {
        self.queue_final_stop(agent_id, stop_id, true);
    }

    /// Whether final stop `stop_id` waits out its grace.
    #[cfg(any(test, feature = "test-support"))]
    pub fn in_grace_for_test(&self, stop_id: StopId) -> bool {
        lock(&self.in_grace).contains(&stop_id)
    }

    /// Whether a task is still resending stop `stop_id`.
    #[cfg(any(test, feature = "test-support"))]
    pub fn resending_for_test(&self, stop_id: StopId) -> bool {
        lock(&self.resending).contains_key(&stop_id)
    }

    /// Settle the final stops `records` listed earlier, as a load does.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn settle_listed_for_test(
        &self,
        records: Vec<intent_core::remote_launch::RemoteFinalStopRecord>,
    ) {
        self.settle_listed_final_stops(records).await;
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn has_pending_stops(&self) -> bool {
        !lock(&self.stops).is_empty()
    }

    /// Dial `target` (the routes, port and pin a refreshed registration
    /// was accepted on) from now on.
    pub fn set_target(&self, mut target: MachineTarget) {
        let mut current = lock(&self.target);
        if target.gate.is_none() {
            target.gate.clone_from(&current.gate);
        }
        *current = target;
    }

    pub fn target(&self) -> MachineTarget {
        lock(&self.target).clone()
    }

    pub fn machine_id(&self) -> String {
        lock(&self.status).node_id.clone()
    }

    /// The lease this connector binds: its journal's sequences are this
    /// lease's.
    pub fn lease_id(&self) -> String {
        lock(&self.status).lease_id.clone()
    }

    fn welcome(&self, welcome: &LinkWelcome) {
        self.protocol
            .store(welcome.protocol, std::sync::atomic::Ordering::SeqCst);
        let mut status = lock(&self.status);
        status.connected = true;
        status.running_agents.clone_from(&welcome.running_agents);
        drop(status);
        self.capabilities(&welcome.capabilities);
        if !welcome.hosts.is_empty() || welcome.tc_address.is_some() {
            let mut target = lock(&self.target);
            target.hosts.clone_from(&welcome.hosts);
            // The tailcat address is stable: a bind while the machine's
            // tunnel is down keeps the last known one.
            if welcome.tc_address.is_some() {
                target.tc_address.clone_from(&welcome.tc_address);
            }
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
    pub fn availability(&self) -> PlatformAvailability {
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
                .record_exposed_machine_protocol(&id, welcome.protocol)
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
            ConnectFailure::Local(_)
            | ConnectFailure::Cloud { .. }
            | ConnectFailure::Orchestrator => return,
        };
        let id = self.machine_id();
        if let Err(error) = self.store.set_exposed_machine_state(&id, state).await {
            tracing::warn!(%error, machine = %id, "remote machine: could not record its state");
        }
    }

    /// Publish `remoteMachine:changed` with the machine as it is now.
    pub async fn announce(&self) {
        if let Some(inbound) = &self.inbound {
            inbound.machine_changed(self).await;
        }
    }

    /// Whether a link is up now.
    pub fn is_connected(&self) -> bool {
        lock(&self.status).connected
    }
}

impl LinkView for Shared {
    fn machine_id(&self) -> String {
        Shared::machine_id(self)
    }

    fn is_connected(&self) -> bool {
        Shared::is_connected(self)
    }

    fn live_agents(&self) -> usize {
        self.agents.live_count()
    }

    fn protocol(&self) -> u32 {
        Shared::protocol(self)
    }

    fn current_binding(&self) -> Option<intent_core::node_link::LeaseBinding> {
        self.current_link().map(|link| link.binding().clone())
    }

    fn lease_id(&self) -> String {
        Shared::lease_id(self)
    }

    fn set_holds_homes(&self, holds: bool) {
        Shared::set_holds_homes(self, holds);
    }

    fn home_backed_up(&self, home_id: &intent_core::HomeId) {
        self.home_link.backed_up(home_id);
    }

    fn send_home_stop<'a>(
        &'a self,
        stop: &'a intent_core::node_link::HomeStopRecord,
    ) -> intent_core::BoxFuture<'a, ()> {
        Box::pin(crate::link::homes::send_stop(self, stop))
    }

    fn resend_home_stops(&self) {
        if let Some(shared) = self.weak().upgrade() {
            crate::link::homes::ensure_stop_resends(&shared);
        }
    }

    fn watch_backup_warning(
        &self,
        home_id: &intent_core::HomeId,
        wait: intent_core::BoxFuture<'static, ()>,
    ) {
        self.home_link.watch_warning(home_id, wait);
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

/// The output a remote agent's setup is kept with (`setupOutput`).
const SETUP_OUTPUT_TAIL_BYTES: usize = 256 * 1024;

/// While output arrives, `agent:updated` goes out at most this often.
const SETUP_OUTPUT_PUBLISH_EVERY: Duration = Duration::from_secs(2);

/// One agent's kept setup output.
#[derive(Default)]
struct SetupTail {
    text: String,
    truncated: bool,
    published: Option<Instant>,
}

impl Shared {
    /// Keep a `setupOutput` of a home's checkout as its `setup.output`
    /// (nodes.md "Opening the home") and announce it, throttled. Output of
    /// a home this backend saw no open of is dropped.
    async fn home_setup_output(
        &self,
        home: &intent_core::node_link::HomeRef,
        seq: u64,
        data: &str,
    ) {
        if let Some(inbound) = &self.inbound {
            inbound.home_setup_output(home, seq, data).await;
        }
    }

    /// Keep a `setupOutput` of an agent attached here (seq 0 starts the
    /// output over) and announce it, throttled.
    async fn setup_output(&self, output: intent_core::node_link::SetupOutput) {
        let agent_id = match &output.owner {
            intent_core::node_link::CheckoutOwner::Agent(agent_id) => agent_id,
            intent_core::node_link::CheckoutOwner::Home(home) => {
                self.home_setup_output(home, output.seq, &output.data).await;
                return;
            }
        };
        let agent_id = agent_id.clone();
        let Some(workspace_id) = self.agents.workspace_of(&agent_id) else {
            tracing::debug!(agent = %agent_id, "remote machine: setup output for an agent not attached here; dropped");
            return;
        };
        let publish = {
            let mut tails = lock(&self.setup_outputs);
            let tail = tails.entry(agent_id.clone()).or_default();
            if output.seq == 0 {
                tail.text.clear();
                tail.truncated = false;
            }
            tail.text.push_str(&output.data);
            if tail.text.len() > SETUP_OUTPUT_TAIL_BYTES {
                let mut cut = tail.text.len() - SETUP_OUTPUT_TAIL_BYTES;
                while !tail.text.is_char_boundary(cut) {
                    cut += 1;
                }
                tail.text.drain(..cut);
                tail.truncated = true;
            }
            let due = tail
                .published
                .is_none_or(|at| at.elapsed() >= SETUP_OUTPUT_PUBLISH_EVERY);
            if due {
                tail.published = Some(Instant::now());
            }
            due
        };
        if publish {
            if let Some(inbound) = &self.inbound {
                inbound
                    .agent_placement_changed(&workspace_id, &agent_id)
                    .await;
            }
        }
    }

    /// `agent_id`'s kept setup output and whether earlier output was cut.
    pub fn setup_output_tail(&self, agent_id: &AgentId) -> Option<(String, bool)> {
        lock(&self.setup_outputs)
            .get(agent_id)
            .map(|tail| (tail.text.clone(), tail.truncated))
    }
}

impl RemoteNode {
    /// Start the connector to `target` on this backend's live lease there.
    /// Nothing is dialed until a link is wanted. Forwarded `workspace_api`
    /// calls are answered and changes announced with `inbound`; without
    /// them, every machine request is refused.
    ///
    /// # Errors
    ///
    /// When the machine's lease cannot be opened or read.
    pub async fn start(
        store: Store,
        target: MachineTarget,
        tuning: Tuning,
        inbound: Option<Arc<dyn NodeInbound>>,
        forge: Option<Arc<crate::link::forge_token::ForgeTokens>>,
    ) -> Result<Self> {
        let ownership = crate::lease::ownership(&store, &tuning);
        let lease = HeadLease::open(store, &target.id, ownership).await?;
        #[cfg(any(test, feature = "test-support"))]
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
            inbound,
            target: Mutex::new(target),
            parked: Mutex::new(HashMap::new()),
            link: watch::channel(None).0,
            status: Mutex::new(status),
            stops: Mutex::new(BTreeMap::new()),
            in_grace: Mutex::new(std::collections::HashSet::new()),
            settled_finals: Mutex::new(std::collections::HashSet::new()),
            deferred_finals: Mutex::new(std::collections::HashSet::new()),
            setup_outputs: Mutex::default(),
            self_ref: Mutex::new(std::sync::Weak::new()),
            protocol: std::sync::atomic::AtomicU32::new(0),
            prepared_checkout: std::sync::atomic::AtomicBool::new(
                intent_core::node_link::offers_prepared_checkout(),
            ),
            stops_changed: watch::channel(0).0,
            resending: Mutex::new(HashMap::new()),
            stop_exits: Mutex::new(HashMap::new()),
            lost_processes: Mutex::default(),
            last_activity: Mutex::new(Instant::now()),
            use_gate: Arc::default(),
            #[cfg(any(test, feature = "test-support"))]
            final_settles: Mutex::default(),
            #[cfg(any(test, feature = "test-support"))]
            final_begins: Mutex::default(),
            route: Mutex::new(None),
            claimed: std::sync::atomic::AtomicBool::new(false),
            claim_lock: tokio::sync::Mutex::new(()),
            survivors_unclaimed: std::sync::atomic::AtomicBool::new(false),
            capability_waiters: Mutex::new(Vec::new()),
            skills_waiters: Mutex::new(HashMap::new()),
            push_waiters: Mutex::new(HashMap::new()),
            checkout_waiters: Mutex::new(HashMap::new()),
            one_shot_waiters: Mutex::new(HashMap::new()),
            home_waiters: Mutex::new(HashMap::new()),
            home_call_waiters: Mutex::new(HashMap::new()),
            holds_homes: std::sync::atomic::AtomicBool::new(false),
            home_link: crate::link::home_state::HomeLink::default(),
            home_stop_resends: std::sync::atomic::AtomicBool::new(false),
            terminals: crate::link::terminals::HomeTerminals::default(),
            home_scripts: crate::link::home_scripts::HomeScripts::default(),
            secret_waiters: Mutex::new(HashMap::new()),
            secret_forgets: Mutex::new(Vec::new()),
            secret_locks: Mutex::new(HashMap::new()),
            forge,
            forge_confirmed: Mutex::default(),
            forge_answers: watch::channel(0).0,
            ingested: Mutex::new(acked),
            cloud_refusal: watch::channel(None).0,
            demand,
            shutdown: watch::channel(false).0,
        });
        // After a backend restart, agents placed here may still run: keep
        // dialing (with backoff) until a bind claims them, then let the idle
        // timer close an unneeded link.
        *lock(&shared.self_ref) = Arc::downgrade(&shared);
        let lease_id = lease.owner().lease_id.clone();
        shared.load_final_stops().await;
        if has_live_records(&shared.store, &lease_id).await? {
            shared
                .survivors_unclaimed
                .store(true, std::sync::atomic::Ordering::SeqCst);
            // A paused cloud machine is claimed once something asks for it;
            // starting its connector does not resume it.
            if !shared.target().gate.is_some_and(|gate| gate.parked()) {
                shared.wake();
            }
        }
        intent_core::spawn_daemon(run(shared.clone(), lease, Ingest::new(acked), demands));
        intent_core::spawn_daemon(expire_final_stops(Arc::downgrade(&shared)));
        Ok(Self { shared })
    }

    #[must_use]
    pub fn shared(&self) -> Arc<Shared> {
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
        // A cloud machine's dial resumes its sandbox first, which takes as
        // long as the provider and the sandbox scripts allow: its caller
        // waits for the connector, which answers every waiter when the dial
        // ends or drops it when it stops. A shorter wait reported a refused
        // resume (relay-unreachable) as unreachable routes (intent#9825).
        let answer = if self.shared.target().gate.is_some() {
            rx.await
        } else {
            let limit = self.shared.tuning.connect.total + Duration::from_secs(5);
            match tokio::time::timeout(limit, rx).await {
                Ok(answer) => answer,
                Err(_) => return Err(ConnectFailure::Unreachable(Vec::new())),
            }
        };
        answer.unwrap_or_else(|_| Err(ConnectFailure::Local("the connector has stopped".into())))
    }

    /// The label of the route the current (or last) link bound on.
    #[must_use]
    pub fn route(&self) -> Option<String> {
        lock(&self.shared.route).clone()
    }

    /// Whether a bind in this process has claimed the machine's surviving
    /// agents, so [`Self::running`] counts them.
    #[must_use]
    pub fn has_claimed(&self) -> bool {
        self.shared
            .claimed
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Claim the agents the last welcome reported, if no claim has
    /// succeeded yet (the bind's could not read this lease's records).
    /// Stops it queues go out on the live link. Returns whether the
    /// survivors are now claimed.
    pub async fn reclaim(&self) -> bool {
        if self.has_claimed() {
            return true;
        }
        let running = lock(&self.shared.status).running_agents.clone();
        if !claim(&self.shared, &running).await {
            return false;
        }
        self.shared
            .claimed
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(link) = self.shared.current_link() {
            self.shared.resend_queued(&link);
        }
        true
    }

    /// What the machine says about its skills (node-link.md § Skills on the
    /// machine), connecting on demand: with `query`, its user skills;
    /// without, only whether it is reachable. Bounded by the capabilities
    /// timeout.
    pub async fn machine_skills(&self, query: bool) -> MachineSkills {
        let query_id = LinkId::new_v4();
        let ask = async {
            if self.connect().await.is_err() {
                return MachineSkills::Offline;
            }
            if self.shared.protocol() < SKILLS_PROTOCOL {
                return MachineSkills::UpdateRequired;
            }
            if !query {
                return MachineSkills::Reachable;
            }
            let (tx, rx) = oneshot::channel();
            lock(&self.shared.skills_waiters).insert(query_id, tx);
            let frame = LinkFrame::Skills(SkillsQuery { query_id });
            if self.shared.send(&frame).await.is_err() {
                return MachineSkills::Offline;
            }
            rx.await
                .map_or(MachineSkills::Offline, MachineSkills::Listed)
        };
        // One deadline for connecting and answering (agent-aux.md: 10 s).
        let answer = tokio::time::timeout(self.shared.tuning.capabilities_timeout, ask)
            .await
            .unwrap_or(MachineSkills::Offline);
        lock(&self.shared.skills_waiters).remove(&query_id);
        answer
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

    /// Send one secret frame for `agent_id` (nodeProtocol 14, node-link.md
    /// § Secrets on the machine), connecting on demand, and wait for the
    /// machine's answer; one deadline of `wait` for both. A forget that
    /// gets no answer is queued for the next bind.
    pub async fn machine_secret(
        &self,
        agent_id: &AgentId,
        op: MachineSecretOp,
        wait: Duration,
    ) -> MachineSecretAnswer {
        // Every secret operation of the agent, and the bind's flush of its
        // queued forgets, run one at a time.
        let serial = self.shared.secret_lock(agent_id);
        let _serial = serial.lock().await;
        let request_id = LinkId::new_v4();
        let forget = match &op {
            MachineSecretOp::Forget { name } => Some(name.clone()),
            MachineSecretOp::Put { .. } | MachineSecretOp::List => None,
        };
        let frame = match op {
            MachineSecretOp::Put { name, value } => LinkFrame::SecretPut(SecretPut {
                request_id,
                agent_id: agent_id.clone(),
                name,
                value: value.into(),
            }),
            MachineSecretOp::Forget { name } => LinkFrame::SecretForget(SecretForget {
                request_id,
                agent_id: agent_id.clone(),
                name,
            }),
            MachineSecretOp::List => LinkFrame::SecretList(SecretList {
                request_id,
                agent_id: agent_id.clone(),
            }),
        };
        let (tx, rx) = oneshot::channel();
        lock(&self.shared.secret_waiters).insert(request_id, tx);
        let put_name = match &frame {
            LinkFrame::SecretPut(put) => Some(put.name.clone()),
            _ => None,
        };
        let ask = async {
            if self.connect().await.is_err() || self.shared.protocol() < SECRETS_PROTOCOL {
                return None;
            }
            let link = self.shared.current_link()?;
            // A queued forget must never wipe a newer value: the agent's
            // queued forgets go first, those of a name being put are dropped.
            if !self
                .shared
                .flush_secret_forgets(&link, agent_id, put_name.as_deref())
                .await
            {
                return None;
            }
            link.send(&frame).await.ok()?;
            rx.await.ok()
        };
        let reply = tokio::time::timeout(wait, ask).await.ok().flatten();
        lock(&self.shared.secret_waiters).remove(&request_id);
        // The value's only copy on this side: wiped now.
        drop(frame);
        let Some(reply) = reply else {
            if let Some(name) = forget {
                lock(&self.shared.secret_forgets).push((agent_id.clone(), name));
                return MachineSecretAnswer::Queued;
            }
            return MachineSecretAnswer::Offline;
        };
        match (reply.outcome, forget, reply.names) {
            (SecretOutcome::Failed, _, _) => {
                MachineSecretAnswer::Failed(reply.error.unwrap_or_else(|| "failed".to_owned()))
            }
            (SecretOutcome::Ok, Some(_), _) => {
                MachineSecretAnswer::Forgotten(reply.forgotten.unwrap_or(false))
            }
            (SecretOutcome::Ok, None, Some(names)) => MachineSecretAnswer::Names(names),
            (SecretOutcome::Ok, None, None) => MachineSecretAnswer::Stored,
        }
    }

    /// Ask the machine to push `agent_id`'s checkout now (`pushWip`,
    /// nodeProtocol 13) and wait up to `wait` for its answer.
    ///
    /// # Errors
    ///
    /// As [`RemoteNode::connect`]; `node-link-timeout` when the machine
    /// does not answer in time.
    pub async fn push_wip(&self, agent_id: &AgentId, wait: Duration) -> Result<PushWipResult> {
        let request_id = intent_core::node_link::LinkId::new_v4();
        let (tx, rx) = oneshot::channel();
        lock(&self.shared.push_waiters).insert(request_id, tx);
        let reply = async {
            self.connect().await?;
            self.shared
                .send(&LinkFrame::PushWip(PushWipRequest {
                    request_id,
                    agent_id: agent_id.clone(),
                }))
                .await?;
            tokio::time::timeout(wait, rx)
                .await
                .map_err(|_| {
                    Error::Internal(format!(
                        "{}: the machine did not answer the push request",
                        link::codes::TIMEOUT
                    ))
                })?
                .map_err(|_| Error::Internal(link::codes::CLOSED.into()))
        }
        .await;
        lock(&self.shared.push_waiters).remove(&request_id);
        reply
    }

    /// Read one piece of a file in `read.agent_id`'s checkout on the machine
    /// (`checkoutRead`, nodeProtocol 16), waiting up to `wait` for the
    /// answer. Connecting resumes a paused cloud machine.
    ///
    /// # Errors
    ///
    /// As [`RemoteNode::connect`]; `node-link-timeout` when the machine
    /// does not answer in time.
    pub async fn checkout_read(
        &self,
        read: intent_core::node_link::CheckoutRead,
        wait: Duration,
    ) -> Result<intent_core::node_link::CheckoutReadReply> {
        let request_id = read.request_id;
        let (tx, rx) = oneshot::channel();
        lock(&self.shared.checkout_waiters).insert(request_id, tx);
        let reply = async {
            self.connect().await?;
            self.shared.send(&LinkFrame::CheckoutRead(read)).await?;
            tokio::time::timeout(wait, rx)
                .await
                .map_err(|_| {
                    Error::Internal(format!(
                        "{}: the machine did not answer the file read",
                        link::codes::TIMEOUT
                    ))
                })?
                .map_err(|_| Error::Internal(link::codes::CLOSED.into()))
        }
        .await;
        lock(&self.shared.checkout_waiters).remove(&request_id);
        reply
    }

    /// Wait until none of `agents` has an unacknowledged stop, or until
    /// `deadline`; returns those still unacknowledged.
    pub async fn wait_stops(&self, agents: &[AgentId], deadline: Instant) -> Vec<AgentId> {
        self.shared.wait_stops(agents, deadline).await
    }

    /// Run one quick action on the machine (`oneShot`, nodeProtocol 18),
    /// waiting up to `wait` for its reply. When this future is dropped, or
    /// `wait` runs out, the machine is told to end the run
    /// (`oneShotCancel`). The link must already be up.
    ///
    /// # Errors
    ///
    /// The send error; `node-link-timeout` when the machine does not answer
    /// in time; `node-link-closed` when the link drops first.
    pub async fn one_shot(
        &self,
        frame: intent_core::node_link::OneShot,
        wait: Duration,
    ) -> Result<intent_core::node_link::OneShotReply> {
        /// Forgets the waiter, and cancels the run unless it answered.
        struct Pending {
            shared: Arc<Shared>,
            request_id: LinkId,
            answered: bool,
        }
        impl Drop for Pending {
            fn drop(&mut self) {
                lock(&self.shared.one_shot_waiters).remove(&self.request_id);
                if self.answered {
                    return;
                }
                let shared = self.shared.clone();
                let cancel = LinkFrame::OneShotCancel(intent_core::node_link::OneShotCancel {
                    request_id: self.request_id,
                });
                // caller-binding: allow — best-effort cancel of an abandoned quick action on its link
                tokio::spawn(async move {
                    let _ = shared.send(&cancel).await;
                });
            }
        }
        let request_id = frame.request_id;
        let (tx, rx) = oneshot::channel();
        lock(&self.shared.one_shot_waiters).insert(request_id, tx);
        let mut pending = Pending {
            shared: self.shared.clone(),
            request_id,
            answered: false,
        };
        self.shared.send(&LinkFrame::OneShot(frame)).await?;
        let reply = tokio::time::timeout(wait, rx)
            .await
            .map_err(|_| {
                Error::Internal(format!(
                    "{}: the machine did not answer the quick action",
                    link::codes::TIMEOUT
                ))
            })?
            .map_err(|_| Error::Internal(link::codes::CLOSED.into()))?;
        pending.answered = true;
        Ok(reply)
    }

    /// Run one wire method on a home (`homeCall`, nodeProtocol 19), waiting
    /// up to `wait` for its reply. When this future is dropped, or `wait`
    /// runs out, the machine is told to end the call (`homeCallCancel`).
    /// The link must already be up.
    ///
    /// # Errors
    ///
    /// The send error; `node-link-timeout` when the machine does not answer
    /// in time; `node-link-closed` when the link drops first.
    pub async fn home_call(
        &self,
        frame: intent_core::node_link::HomeCall,
        wait: Duration,
    ) -> Result<intent_core::node_link::HomeCallReply> {
        /// Forgets the waiter, and cancels the call unless it answered.
        struct Pending {
            shared: Arc<Shared>,
            request_id: LinkId,
            answered: bool,
        }
        impl Drop for Pending {
            fn drop(&mut self) {
                lock(&self.shared.home_call_waiters).remove(&self.request_id);
                if self.answered {
                    return;
                }
                let shared = self.shared.clone();
                let cancel = LinkFrame::HomeCallCancel(intent_core::node_link::HomeCallCancel {
                    request_id: self.request_id,
                });
                // caller-binding: allow — best-effort cancel of an abandoned home call on its link
                tokio::spawn(async move {
                    let _ = shared.send(&cancel).await;
                });
            }
        }
        let request_id = frame.request_id;
        // App file and git use of a cloud home counts as activity for its
        // idle pause (nodes.md "Cloud homes" 3).
        self.shared.note_activity();
        let (tx, rx) = oneshot::channel();
        lock(&self.shared.home_call_waiters).insert(request_id, tx);
        let mut pending = Pending {
            shared: self.shared.clone(),
            request_id,
            answered: false,
        };
        self.shared.send(&LinkFrame::HomeCall(frame)).await?;
        let reply = tokio::time::timeout(wait, rx)
            .await
            .map_err(|_| {
                Error::Internal(format!(
                    "{}: the machine did not answer the home call",
                    link::codes::TIMEOUT
                ))
            })?
            .map_err(|_| Error::Internal(link::codes::CLOSED.into()))?;
        pending.answered = true;
        Ok(reply)
    }

    /// Send a home frame (`homeOpen` or `homePush`, nodeProtocol 19) whose
    /// request id is `request_id`, connecting first, and wait up to `wait`
    /// for its reply.
    ///
    /// # Errors
    ///
    /// As [`RemoteNode::connect`]; `node-link-timeout` when the machine does
    /// not answer in time; `node-link-closed` when the link drops first.
    pub async fn home_request(
        &self,
        request_id: LinkId,
        frame: LinkFrame,
        wait: Duration,
    ) -> Result<crate::link::homes::HomeReply> {
        let (tx, rx) = oneshot::channel();
        lock(&self.shared.home_waiters).insert(request_id, tx);
        let reply = async {
            self.connect().await?;
            self.shared.send(&frame).await?;
            tokio::time::timeout(wait, rx)
                .await
                .map_err(|_| {
                    Error::Internal(format!(
                        "{}: the machine did not answer the home request",
                        link::codes::TIMEOUT
                    ))
                })?
                .map_err(|_| Error::Internal(link::codes::CLOSED.into()))
        }
        .await;
        lock(&self.shared.home_waiters).remove(&request_id);
        reply
    }

    /// Send a control frame (such as a launch) on the current link.
    ///
    /// # Errors
    ///
    /// `node-link-unavailable` when no link is up; the link's send error.
    pub async fn send(&self, frame: &LinkFrame) -> Result<()> {
        self.shared.send(frame).await
    }

    /// End `agent_id` for good with `reason` (see [`Shared::final_stop`]).
    pub async fn final_stop(
        &self,
        agent_id: &AgentId,
        reason: intent_core::remote_launch::FinalStopReason,
    ) -> StopId {
        self.shared.final_stop(agent_id, reason).await
    }

    /// Wait, up to `bound`, until no prompt waits for a turn of
    /// `agent_id`, which retired itself during that turn; a bind meanwhile
    /// leaves it running. Whether its final stop is still to send: not when
    /// another final stop (a delete, a machine removal) came first.
    pub async fn await_turn_before_final_stop(&self, agent_id: &AgentId, bound: Duration) -> bool {
        lock(&self.shared.deferred_finals).insert(agent_id.clone());
        let _ = tokio::time::timeout(bound, self.shared.agents.turns_over(agent_id)).await;
        lock(&self.shared.deferred_finals).remove(agent_id);
        if self.shared.final_stop_queued(agent_id) {
            return false;
        }
        !matches!(
            self.shared.store.get_final_stop(agent_id).await,
            Ok(Some(_))
        )
    }

    /// Agents head counts as running on the machine.
    #[must_use]
    pub fn running(&self) -> usize {
        self.shared.agents.live_count()
    }

    /// End every agent attached here for good (the machine is being
    /// removed): their runtimes end and the machine is sent final stops.
    /// Returns every agent with a stop the machine has not acknowledged
    /// yet, these included.
    #[must_use = "the stops are only confirmed once acknowledged"]
    pub async fn stop_all(&self) -> Vec<AgentId> {
        let mut stopped = self.shared.agents.live_ids();
        for agent_id in &stopped {
            self.shared
                .final_stop(
                    agent_id,
                    intent_core::remote_launch::FinalStopReason::MachineRemoved,
                )
                .await;
        }
        for agent_id in lock(&self.shared.stops).keys() {
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

    /// The link as the domain reads it.
    #[must_use]
    pub fn view(&self) -> &dyn LinkView {
        self.shared.as_ref()
    }

    /// The connector's internal state. Domain code calls the methods on
    /// `RemoteNode`, [`GateLink`](crate::link::gate::GateLink) and
    /// [`view`](Self::view) instead.
    #[must_use]
    pub fn shared_ref(&self) -> &Shared {
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
    pub async fn launch(&self, mut command: LaunchCommand) -> Result<Arc<RemoteAgentRuntime>> {
        let parked = lock(&self.shared.parked).remove(&command.agent_id);
        // A parked runtime is the stopping process's while a stop for it is
        // queued: the relaunch waits for that stop below instead.
        if let Some(runtime) = parked
            .filter(|runtime| runtime.is_running() && !self.shared.stop_queued(&command.agent_id))
        {
            return Ok(runtime);
        }
        // Never launch behind this agent's unacknowledged stop: a resend of
        // it would stop the new process. Attach only after the stopped
        // process's exit record is in, so it never ends the new runtime.
        self.shared.no_stop_pending(&command.agent_id).await?;
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
        let link = if command.forge.is_some() && link.protocol() >= FORGE_TOKEN_PROTOCOL {
            let ready = self.shared.forge_ready(FORGE_LAUNCH_WAIT).await;
            // A rebind while waiting: the answer is the new bind's, so the
            // launch goes on that link, or without forge if it is older.
            let current = self.shared.current_link().unwrap_or(link);
            if !ready || current.protocol() < FORGE_TOKEN_PROTOCOL {
                command.forge = None;
            }
            current
        } else {
            command.forge = None;
            link
        };
        if link.protocol() < FORGE_FETCH_PROTOCOL || command.forge.is_some() {
            command.forge_fetch = None;
        }
        if command.forge_fetch.is_none() {
            command.forge_fetch_modules.clear();
        }
        if link.protocol() < SESSION_META_PROTOCOL {
            command.session_meta = None;
        }
        let session_prompt = command
            .session_meta
            .as_ref()
            .is_some_and(|meta| meta.contains_key("systemPrompt"));
        link.send(&LinkFrame::Launch(command)).await?;
        if session_prompt {
            runtime.set_session_prompt();
        }
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

/// What workspace homes, home terminals and home scripts ask of their
/// machine's connector.
impl RemoteNode {
    /// Take a use of the machine: an idle pause waits for it to end.
    #[must_use = "the use ends when the guard drops"]
    pub async fn begin_use(&self) -> tokio::sync::OwnedRwLockReadGuard<()> {
        self.shared.begin_use().await
    }

    /// The link went away: detach the machine's homes after the link-loss
    /// grace unless a link comes back.
    pub fn home_link_lost(&self) {
        self.shared
            .home_link
            .link_lost(self.shared.weak(), self.shared.tuning.link_loss_grace);
    }

    /// Open a terminal in `home` on the machine.
    ///
    /// # Errors
    ///
    /// When the machine is not connected or the terminal cannot start.
    pub fn open_home_terminal(
        &self,
        home: &intent_core::WorkspaceHome,
        workspace_id: &WorkspaceId,
        pty: &Arc<intent_pty::PtyHost>,
        open: intent_pty::home::HomeTerminalOpen,
    ) -> Result<intent_pty::PtyId> {
        crate::link::terminals::open_on(&self.shared, home, workspace_id, pty, open)
    }

    /// Run a command in `home` on the machine, without a PTY.
    #[must_use]
    pub fn open_home_exec(
        &self,
        home: &intent_core::WorkspaceHome,
        workspace_id: &WorkspaceId,
        open: intent_pty::home::HomeExecOpen,
    ) -> intent_pty::home::HomeExec {
        crate::link::terminals::open_exec_on(&self.shared, home, workspace_id, open)
    }

    /// End every terminal of `home_id` on the machine.
    pub async fn end_home_terminals(&self, home_id: &HomeId) {
        self.shared.terminals.end_home(&self.shared, home_id).await;
    }

    /// Start a script run in `home` on the machine.
    ///
    /// # Errors
    ///
    /// When the machine is not connected or the run cannot start.
    pub fn start_home_script(
        &self,
        home: &intent_core::WorkspaceHome,
        workspace_id: &WorkspaceId,
        pty: &Arc<intent_pty::PtyHost>,
        open: intent_pty::home::HomeScriptOpen,
    ) -> Result<Option<intent_pty::PtyId>> {
        crate::link::home_scripts::start_on(&self.shared, home, workspace_id, pty, open)
    }

    /// Stop the home script run `id`, waiting up to `wait`.
    pub async fn stop_home_script(
        &self,
        pty: &Arc<intent_pty::PtyHost>,
        id: intent_pty::PtyId,
        wait: Duration,
    ) -> crate::link::home_scripts::Stopped {
        self.shared.home_scripts.stop(pty, id, wait).await
    }
}

/// What launching, removing and retargeting a machine ask of its
/// connector.
impl RemoteNode {
    /// The current link, if one is up.
    #[must_use]
    pub fn current_link(&self) -> Option<NodeLink> {
        self.shared.current_link()
    }

    /// The node protocol of the current or last link.
    #[must_use]
    pub fn protocol(&self) -> u32 {
        self.shared.protocol()
    }

    /// Whether the machine prepares checkouts itself.
    #[must_use]
    pub fn offers_prepared_checkout(&self) -> bool {
        self.shared.offers_prepared_checkout()
    }

    /// Live attached agents.
    #[must_use]
    pub fn live_agents(&self) -> usize {
        self.shared.agents.live_count()
    }

    /// `agent_id` is being launched: a stop queued for it no longer holds.
    pub fn launching(&self, agent_id: &AgentId) {
        self.shared.launching(agent_id);
    }

    /// Whether `agent_id`'s parked runtime is still running, so a launch
    /// reattaches to it.
    #[must_use]
    pub fn reattaches(&self, agent_id: &AgentId) -> bool {
        self.shared.reattaches(agent_id)
    }

    /// The tail of `agent_id`'s setup output, and whether it was cut.
    #[must_use]
    pub fn setup_output_tail(&self, agent_id: &AgentId) -> Option<(String, bool)> {
        self.shared.setup_output_tail(agent_id)
    }

    /// How long a removal waits for the machine's stops.
    #[must_use]
    pub fn remove_budget(&self) -> Duration {
        self.shared.remove_budget()
    }

    /// Stop `agents`' script runs on the machine by `deadline`; the agents
    /// whose stop was not acknowledged.
    pub async fn end_agents_machine_runs(
        &self,
        agents: &[AgentId],
        deadline: Instant,
        reason: &str,
    ) -> Vec<AgentId> {
        self.shared
            .end_agents_machine_runs(agents, deadline, reason)
            .await
    }
}

/// What a cloud machine's lifecycle (pause, resume, idle reaping) asks of
/// its connector.
impl RemoteNode {
    /// Live attached agents with a turn in flight.
    #[must_use]
    pub fn turns_in_flight(&self) -> usize {
        self.shared.agents.turns_in_flight()
    }

    /// Close the current link without a redial.
    pub fn close_link(&self) {
        self.shared.close_link();
    }

    /// Whether something would keep a link up now.
    #[must_use]
    pub fn link_wanted(&self) -> bool {
        self.shared.link_wanted()
    }

    /// Ask for a link without waiting.
    pub fn wake(&self) {
        self.shared.wake();
    }

    /// `agents`' processes are gone: their runtimes end with `error`.
    pub fn processes_lost(&self, agents: &[AgentId], error: &str) {
        self.shared.processes_lost(agents, error);
    }

    /// Whether the machine has work now; one without any may be paused.
    #[must_use]
    pub fn busy(&self) -> bool {
        self.shared.busy()
    }

    /// When the machine last had agent activity.
    #[must_use]
    pub fn last_activity(&self) -> Instant {
        self.shared.last_activity()
    }

    /// Wait up to `wait` for the uses in flight to end and hold off new
    /// ones while the guard lives; `None` when uses kept it busy.
    pub async fn quiesce(&self, wait: Duration) -> Option<tokio::sync::OwnedRwLockWriteGuard<()>> {
        self.shared.quiesce(wait).await
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

/// Every [`Tuning::final_stop_check`], drop the recorded final stops past
/// their bound, bound to the machine or not, so a machine that stays away
/// is no longer redialed for them. Ends with the connector.
async fn expire_final_stops(shared: std::sync::Weak<Shared>) {
    let Some(every) = shared
        .upgrade()
        .map(|shared| shared.tuning.final_stop_check)
    else {
        return;
    };
    let mut tick = tokio::time::interval(every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await;
    loop {
        let Some(mut shutdown) = shared.upgrade().map(|shared| shared.shutdown.subscribe()) else {
            return;
        };
        tokio::select! {
            _ = tick.tick() => {}
            () = wait_true(&mut shutdown) => return,
        }
        let Some(current) = shared.upgrade() else {
            return;
        };
        current.load_final_stops().await;
    }
}

async fn run(
    shared: Arc<Shared>,
    lease: HeadLease,
    mut ingest: Ingest,
    mut demands: mpsc::UnboundedReceiver<Option<Waiter>>,
) {
    let mut backoff = shared.tuning.backoff.clone();
    let mut shutdown = shared.shutdown.subscribe();
    let mut waiters: Vec<Waiter> = Vec::new();
    // Redial after a failure or a lost link while the link is wanted.
    let mut retry = false;
    // Whether someone asked for the link since the last dial: a parked
    // (paused) cloud machine is dialed, and so resumed, only then.
    let mut demanded = false;
    loop {
        if !retry && !demanded && waiters.is_empty() && !shared.wants_link() {
            tokio::select! {
                demand = demands.recv() => match demand {
                    Some(waiter) => {
                        demanded = true;
                        waiters.extend(waiter);
                    }
                    None => return,
                },
                () = wait_true(&mut shutdown) => return,
            }
        }
        while let Ok(waiter) = demands.try_recv() {
            demanded = true;
            waiters.extend(waiter);
        }
        // Final stops past their bound are dropped before every dial, so a
        // machine that stays away is no longer redialed for them.
        shared.load_final_stops().await;
        if retry && waiters.is_empty() && !shared.wants_link() {
            retry = false;
            continue;
        }
        // A connector shut down before this dial (even before its loop
        // first ran) binds nothing: its link would claim the lease from
        // the connector that replaced it, then close (intent#8611).
        if *shutdown.borrow() {
            return;
        }
        let target = shared.target();
        if let Some(gate) = &target.gate {
            if gate.parked() && !demanded && waiters.is_empty() {
                // Paused: wait for a prompt, a stop or a connect request.
                retry = false;
                tokio::select! {
                    demand = demands.recv() => match demand {
                        Some(waiter) => {
                            demanded = true;
                            waiters.extend(waiter);
                        }
                        None => return,
                    },
                    () = wait_true(&mut shutdown) => return,
                }
                continue;
            }
        }
        let asked = demanded || !waiters.is_empty();
        demanded = false;
        let version = lock(&shared.status).intentd_version.clone();
        let ingested = ingest.ingested();
        let gated = match &target.gate {
            Some(gate) => tokio::select! {
                gated = gate.before_dial(shared.as_ref(), asked) => gated.map(Some),
                () = wait_true(&mut shutdown) => return,
            },
            None => Ok(None),
        };
        // The cloud machine's pause count when its gate let this dial
        // through; `None` for an exposed machine.
        let mut pauses = None;
        let connected = match gated {
            Ok(seen) => {
                pauses = seen.map(|seen| seen.pauses);
                // The gate may have moved the sandbox to a new address.
                let target = shared.target();
                let next_hello = || {
                    let lease = lease.clone();
                    async move { lease.next_hello(ingested).await }
                };
                let tuning = &shared.tuning.connect;
                let version = version.as_deref();
                let dialing = async {
                    if seen.is_some_and(|seen| seen.resumed) {
                        let gate = target.gate.clone();
                        dial_with_revives(
                            || {
                                let target = shared.target();
                                async move {
                                    connect_after_resume(&target, tuning, version, next_hello).await
                                }
                            },
                            || async {
                                match &gate {
                                    Some(gate) => gate.revive(shared.as_ref()).await,
                                    None => false,
                                }
                            },
                            &target.id,
                        )
                        .await
                    } else {
                        connect(&target, tuning, version, next_hello).await
                    }
                };
                tokio::select! {
                    connected = dialing => connected,
                    () = wait_true(&mut shutdown) => return,
                }
            }
            Err(failure) => Err(failure),
        };
        if *shutdown.borrow() {
            if let Ok(connected) = connected {
                connected.dialed.link.close();
            }
            return;
        }
        let Connected {
            dialed,
            endpoint,
            route,
        } = match connected {
            Ok(connected) => connected,
            Err(failure) => {
                tracing::warn!(machine = %target.id, failure = ?failure, "remote machine: cannot connect");
                if let ConnectFailure::Cloud { message, data, .. } = &failure {
                    let code = data["code"].as_str().unwrap_or("cloud-machine-unavailable");
                    shared
                        .cloud_refusal
                        .send_replace(Some(format!("{code}: {message}")));
                }
                // Persist the resting state before revocation ends agents.
                // `end_all` drops `running()` to zero, and a list between
                // that and this write still reads `disconnected`
                // (intent-hq/intent#8484).
                shared.record_failure(&failure).await;
                if let (ConnectFailure::UpdateRequired(update), Some(gate)) =
                    (&failure, &target.gate)
                {
                    // A cloud sandbox older than this backend never gets an
                    // update: it is deleted. A newer one (this backend was
                    // rolled back) links again once the backend is updated.
                    if update.update == intent_core::machines::UpdateSide::Machine {
                        gate.outdated(shared.as_ref());
                    }
                }
                if matches!(failure, ConnectFailure::Revoked(_)) {
                    // Revocation stopped this binding's agents on the
                    // machine: their stops are moot, and they no longer run.
                    shared.drop_stops();
                    let ended = shared.agents.end_all(&format!(
                        "{}: {} revoked this backend's credential and stopped the agent",
                        crate::connect::MACHINE_REVOKED,
                        target.name
                    ));
                    if ended > 0 {
                        tracing::info!(machine = %target.id, ended, "remote machine: revoked; its agents ended");
                    }
                }
                // Only an exposed machine can become an orchestrator.
                if matches!(failure, ConnectFailure::Orchestrator) && target.gate.is_none() {
                    shared.drop_stops();
                    shared.agents.end_all(&format!(
                        "{}: {} is now an orchestrator and no longer runs agents for other backends",
                        crate::connect::MACHINE_ORCHESTRATOR,
                        target.name
                    ));
                    if let Some(inbound) = &shared.inbound {
                        inbound.machine_became_orchestrator(
                            target.id.clone(),
                            target.token.expose().to_owned(),
                        );
                    }
                } else {
                    shared.announce().await;
                }
                for waiter in waiters.drain(..) {
                    let _ = waiter.send(Err(failure.clone()));
                }
                // Fail the capability requests; they no longer hold a link.
                lock(&shared.capability_waiters).clear();
                lock(&shared.skills_waiters).clear();
                lock(&shared.push_waiters).clear();
                lock(&shared.checkout_waiters).clear();
                lock(&shared.one_shot_waiters).clear();
                lock(&shared.home_waiters).clear();
                lock(&shared.home_call_waiters).clear();
                lock(&shared.secret_waiters).clear();
                retry = shared.wants_link();
                if retry {
                    let delay = match failure {
                        ConnectFailure::Revoked(_)
                        | ConnectFailure::UpdateRequired(_)
                        | ConnectFailure::Orchestrator
                        | ConnectFailure::Cloud { lost: true, .. } => shared.tuning.permanent_retry,
                        _ => backoff.next_delay(),
                    };
                    tokio::select! {
                        () = tokio::time::sleep(delay) => {}
                        demand = demands.recv() => match demand {
                            Some(waiter) => {
                                demanded = true;
                                waiters.extend(waiter);
                            }
                            None => return,
                        },
                        () = wait_true(&mut shutdown) => return,
                    }
                }
                continue;
            }
        };
        // A cloud machine paused while this dial was in flight: the link
        // would be bound to a frozen sandbox. Drop it and dial again through
        // the gate, which resumes the sandbox for an asked dial.
        if let Some(gate) = &target.gate {
            if Some(gate.pauses()) != pauses {
                dialed.link.close();
                demanded = asked;
                retry = shared.wants_link();
                continue;
            }
        }
        let link::DialedLink {
            link,
            events,
            welcome,
        } = dialed;
        retry = false;
        shared.cloud_refusal.send_replace(None);
        *lock(&shared.route) = Some(route);
        let fenced = match lease
            .record_welcome(&welcome, link.binding().generation)
            .await
        {
            Ok(()) => false,
            Err(error) => {
                tracing::warn!(%error, "remote machine: could not record the welcome");
                matches!(error, crate::lease::LinkWrite::Fenced(_))
            }
        };
        let refusal = if fenced {
            // Another process owns this link: write nothing, claim no agent
            // and stop none.
            Some("another backend process owns this machine's link now; nothing was received")
        } else {
            shared.welcome(&welcome);
            shared.record_link(&welcome).await;
            // Before the link is published: queue the stops of agents head no
            // longer claims, then send every queued stop, so they precede any
            // launch or prompt on this link.
            #[cfg(any(test, feature = "test-support"))]
            if let Some(hook) = &shared.tuning.before_claim.0 {
                hook(&shared);
            }
            // The survivors cannot be told apart, so this link is not served:
            // their journal records would have no runtime to go to, yet be
            // acknowledged.
            (!claim(&shared, &welcome.running_agents).await).then_some(
                "could not tell which of this backend's agents still run there (its agent records could not be read); nothing was received",
            )
        };
        if let Some(refusal) = refusal {
            // Close it and bind again (with backoff); nothing was
            // acknowledged, so the machine replays everything on the next
            // bind.
            link.close();
            shared.disconnected();
            let failure = ConnectFailure::Local(refusal.into());
            for waiter in waiters.drain(..) {
                let _ = waiter.send(Err(failure.clone()));
            }
            lock(&shared.capability_waiters).clear();
            lock(&shared.skills_waiters).clear();
            lock(&shared.push_waiters).clear();
            lock(&shared.checkout_waiters).clear();
            lock(&shared.one_shot_waiters).clear();
            lock(&shared.home_waiters).clear();
            lock(&shared.home_call_waiters).clear();
            lock(&shared.secret_waiters).clear();
            shared.announce().await;
            retry = shared.wants_link();
            if retry {
                tokio::select! {
                    () = tokio::time::sleep(backoff.next_delay()) => {}
                    demand = demands.recv() => match demand {
                        Some(waiter) => {
                            demanded = true;
                            waiters.extend(waiter);
                        }
                        None => return,
                    },
                    () = wait_true(&mut shutdown) => return,
                }
            }
            continue;
        }
        backoff.reset();
        shared
            .claimed
            .store(true, std::sync::atomic::Ordering::SeqCst);
        // Before the link is published: a launch goes out as soon as it is,
        // and the node's checkout fetches over the git connection at once
        // (intent-hq/intent#8383). Before the queued stops too, so their
        // resenders start right after they are sent.
        let git = serve_git(&shared, &endpoint, &link).await;
        // Before the link is published, so it precedes every launch on it.
        let forge = match &shared.forge {
            Some(forge) if link.protocol() >= FORGE_TOKEN_PROTOCOL => {
                let mut changes = forge.subscribe();
                // A slow resolution (a stalled `gh`) leaves the last value.
                // It keeps going after the timeout; its value then goes out
                // as a change.
                let _ =
                    tokio::time::timeout(FORGE_BIND_RESOLVE, forge.refresh_in_background()).await;
                let held = changes.borrow_and_update().clone();
                // A bind starts with nothing confirmed.
                *lock(&shared.forge_confirmed) = crate::link::forge_token::Confirmed::default();
                crate::link::forge_token::send(&link, &held, &shared.forge_confirmed).await;
                shared.forge_answers.send_modify(|n| *n = n.wrapping_add(1));
                Some(changes)
            }
            _ => None,
        };
        shared.send_queued(&link).await;
        #[cfg(any(test, feature = "test-support"))]
        if let Some(hook) = &shared.tuning.before_publish.0 {
            hook(&shared);
        }
        shared.link.send_replace(Some(link.clone()));
        // The same check as after the dial, once the link is published: a
        // pause counts before it closes the published link, so either it
        // closed this link or the count shows it began. Nothing stays bound
        // to a frozen sandbox.
        if let Some(gate) = &target.gate {
            if Some(gate.pauses()) != pauses {
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
                shared
                    .terminals
                    .link_lost(shared.weak(), shared.tuning.link_loss_grace);
                shared
                    .home_link
                    .link_lost(shared.weak(), shared.tuning.link_loss_grace);
                demanded = asked;
                retry = shared.wants_link();
                continue;
            }
        }
        shared.resend_queued(&link);
        for waiter in waiters.drain(..) {
            let _ = waiter.send(Ok(()));
        }
        shared.home_link.bound();
        if link.protocol() >= intent_core::node_link::HOMES_PROTOCOL {
            if let Some(inbound) = shared.inbound() {
                inbound.homes_after_bind(shared.clone());
            }
        }
        shared.terminals.after_bind(&link);
        tracing::info!(
            machine = %target.id,
            generation = link.binding().generation.0,
            ingested = ingested.0,
            "remote machine: link bound"
        );
        shared.announce().await;
        let served = Served {
            lease: &lease,
            link: &link,
            forge,
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
        lock(&shared.skills_waiters).clear();
        lock(&shared.push_waiters).clear();
        lock(&shared.checkout_waiters).clear();
        lock(&shared.one_shot_waiters).clear();
        lock(&shared.home_waiters).clear();
        lock(&shared.home_call_waiters).clear();
        lock(&shared.secret_waiters).clear();
        // Home terminals wait out the link-loss grace for the next bind.
        shared
            .terminals
            .link_lost(shared.weak(), shared.tuning.link_loss_grace);
        // Its homes are detached once the same grace passes without a bind.
        shared
            .home_link
            .link_lost(shared.weak(), shared.tuning.link_loss_grace);
        // The machine stops this binding's scripts when its link closes.
        shared
            .home_scripts
            .link_lost(crate::link::home_scripts::LOST_DISCONNECTED);
        shared
            .machine_runs_lost(crate::link::home_scripts::LOST_DISCONNECTED)
            .await;
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
    /// Changes of the GitHub token to send (nodeProtocol 15).
    forge: Option<watch::Receiver<crate::link::forge_token::Held>>,
}

/// Ingest one link's journal, answer its requests and connect requests,
/// until it closes, a live gap needs a rebind, it has been idle for
/// [`Tuning::idle_timeout`], or shutdown.
async fn serve(
    shared: &Arc<Shared>,
    Served {
        lease,
        link,
        mut forge,
    }: Served<'_>,
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
        let due = acks.due();
        tokio::select! {
            event = events.recv() => {
                let Some(event) = event else { break Ended::Lost };
                match event {
                    LinkEvent::Frame(LinkFrame::Journal(record)) => {
                        let seq = record.seq;
                        match ingest.accept(seq) {
                            Step::Ingest => {
                                ingest_record(shared, record).await;
                            }
                            Step::IngestAfterLoss { lost } => {
                                tracing::warn!(
                                    from = lost.0 .0,
                                    through = lost.1 .0,
                                    "remote machine: journal records were lost on the machine"
                                );
                                ingest_record(shared, record).await;
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
                        if acks.record(shared.tuning.ack_interval, shared.tuning.ack_batch) {
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
                    LinkEvent::Frame(LinkFrame::SetupOutput(output)) => {
                        shared.setup_output(output).await;
                    }
                    LinkEvent::Frame(LinkFrame::SkillsReply(reply)) => {
                        if let Some(waiter) = lock(&shared.skills_waiters).remove(&reply.query_id) {
                            let _ = waiter.send(reply.skills);
                        }
                    }
                    LinkEvent::Frame(LinkFrame::OneShotReply(reply)) => {
                        if let Some(waiter) =
                            lock(&shared.one_shot_waiters).remove(&reply.request_id)
                        {
                            let _ = waiter.send(reply);
                        }
                    }
                    LinkEvent::Frame(LinkFrame::HomeOpenReply(reply)) => {
                        if let Some(waiter) = lock(&shared.home_waiters).remove(&reply.request_id) {
                            let _ = waiter.send(crate::link::homes::HomeReply::Open(reply));
                        }
                    }
                    LinkEvent::Frame(LinkFrame::PtyOutput(output)) => {
                        shared.terminals.output(link, output).await;
                    }
                    LinkEvent::Frame(LinkFrame::PtyExited(exited)) => {
                        shared.terminals.exited(&exited);
                    }
                    LinkEvent::Frame(LinkFrame::HomeCallReply(reply)) => {
                        if let Some(waiter) =
                            lock(&shared.home_call_waiters).remove(&reply.request_id)
                        {
                            let _ = waiter.send(reply);
                        }
                    }
                    LinkEvent::Frame(LinkFrame::HomeEvents(events)) => {
                        // In order: one frame's events before the next's.
                        if let Some(inbound) = &shared.inbound {
                            inbound.home_events(&shared.machine_id(), events).await;
                        }
                    }
                    LinkEvent::Frame(LinkFrame::HomeStopAck(ack)) => {
                        let shared = shared.clone();
                        // Off the ingest loop: it writes the store and may
                        // post a notice.
                        intent_core::spawn_daemon(async move {
                            if let Some(inbound) = shared.inbound() {
                                inbound.home_stop_acked(shared.clone(), ack).await;
                            }
                        });
                    }
                    LinkEvent::Frame(LinkFrame::HomePushReply(reply)) => {
                        if let Some(waiter) = lock(&shared.home_waiters).remove(&reply.request_id) {
                            let _ = waiter.send(crate::link::homes::HomeReply::Push(reply));
                        }
                    }
                    LinkEvent::Frame(LinkFrame::CheckoutReadReply(reply)) => {
                        if let Some(waiter) =
                            lock(&shared.checkout_waiters).remove(&reply.request_id)
                        {
                            let _ = waiter.send(reply);
                        }
                    }
                    LinkEvent::Frame(LinkFrame::PushWipReply(reply)) => {
                        if let Some(waiter) = lock(&shared.push_waiters).remove(&reply.request_id) {
                            let _ = waiter.send(reply);
                        }
                    }
                    LinkEvent::Frame(LinkFrame::SecretReply(reply)) => {
                        if let Some(waiter) = lock(&shared.secret_waiters).remove(&reply.request_id)
                        {
                            let _ = waiter.send(reply);
                        }
                    }
                    LinkEvent::Frame(LinkFrame::ForgeTokenReply(reply)) => {
                        let ok = reply.outcome == SecretOutcome::Ok;
                        lock(&shared.forge_confirmed).answer(reply.request_id, ok);
                        shared.forge_answers.send_modify(|n| *n = n.wrapping_add(1));
                        if !ok {
                            // A fixed code only, never the peer's text.
                            let error = match reply.error.as_deref() {
                                Some(intent_core::node_link::FORGE_TOKEN_INVALID) => {
                                    intent_core::node_link::FORGE_TOKEN_INVALID
                                }
                                _ => "failed",
                            };
                            tracing::warn!(error, "remote machine: refused the GitHub token");
                        }
                    }
                    // A home run's frames feed its PTY here; an agent's go to
                    // its machine run.
                    LinkEvent::Frame(frame)
                        if frame.script_owner().is_some_and(|owner| owner.home().is_some()) =>
                    {
                        shared.home_scripts.frame(frame);
                    }
                    LinkEvent::Frame(frame)
                        if frame.channel() == intent_core::node_link::LinkChannel::Script =>
                    {
                        if let Some(inbound) = &shared.inbound {
                            inbound
                                .machine_script_frame(&shared.machine_id(), frame)
                                .await;
                        }
                    }
                    LinkEvent::Frame(frame) => {
                        tracing::debug!(channel = ?frame.channel(), "remote machine: ignoring an unexpected frame");
                    }
                    LinkEvent::Request(request) => match &shared.inbound {
                        Some(inbound) => inbound.link_request(shared.clone(), request),
                        None => answer_unsupported(request),
                    },
                }
            }
            held = crate::link::forge_token::changed(&mut forge) => {
                crate::link::forge_token::send(link, &held, &shared.forge_confirmed).await;
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

/// Whether agents that may still run are recorded on `lease_id`: a record
/// whose session has not ended (deleted or completed). A session that cannot
/// be read counts, so a read failure never hides a survivor.
///
/// # Errors
///
/// When the store cannot be read.
pub async fn has_live_records(store: &Store, lease_id: &str) -> Result<bool> {
    if !store.list_final_stops(lease_id).await?.is_empty() {
        return Ok(true);
    }
    for record in store.list_remote_agents(lease_id).await? {
        match store.get_agent_session_summary(&record.agent_id).await {
            Ok(session) if crate::final_stop::session_ended(&session) => {}
            _ => return Ok(true),
        }
    }
    Ok(false)
}

/// Claim the node's running agents at bind. An agent head has no record of
/// on this lease (a head crash mid-launch, a deleted agent) gets a final
/// stop held in memory, with a new id; one whose session has ended
/// (retired, deleted or completed) gets a recorded final stop; one with a
/// recorded final stop already has it queued. Each is resent until
/// acknowledged, and none is parked; nor is one with a plain stop queued. One head knows but has not attached
/// since it started gets a runtime, kept until the agent manager takes it,
/// so its output is not dropped.
///
/// Returns whether the claim succeeded: `false` when this lease's agent
/// records could not be read, so nothing was claimed. Claims take turns
/// (`claim_lock`), so a retried claim never attaches an agent twice.
async fn claim(shared: &Arc<Shared>, running_agents: &[AgentId]) -> bool {
    let _claiming = shared.claim_lock.lock().await;
    if running_agents.is_empty() {
        return true;
    }
    let lease_id = lock(&shared.status).lease_id.clone();
    #[cfg(any(test, feature = "test-support"))]
    let read = if shared
        .tuning
        .fail_claim_reads
        .load(std::sync::atomic::Ordering::SeqCst)
    {
        Err(intent_core::Error::Internal(
            "test: claim read fails".into(),
        ))
    } else {
        shared.store.list_remote_agents(&lease_id).await
    };
    #[cfg(not(any(test, feature = "test-support")))]
    let read = shared.store.list_remote_agents(&lease_id).await;
    let read = match read {
        Ok(claimed) => shared
            .store
            .list_final_stops(&lease_id)
            .await
            .map(|finals| (claimed, finals)),
        Err(error) => Err(error),
    };
    let (claimed, finals) = match read {
        Ok(read) => read,
        Err(error) => {
            // Without the records head cannot tell which agents are its
            // own; stopping them all could end live work, so claim nothing
            // and report the failure.
            tracing::warn!(%error, "remote node: could not read this lease's agents; claiming nothing");
            return false;
        }
    };
    let records: HashMap<&AgentId, &intent_core::remote_launch::RemoteAgentRecord> = claimed
        .iter()
        .map(|record| (&record.agent_id, record))
        .collect();
    for agent_id in running_agents {
        if let Some(recorded) = finals.iter().find(|stop| &stop.agent_id == agent_id) {
            if let Ok(stop_id) = recorded.stop_id.parse::<StopId>() {
                shared.queue_final_stop(agent_id, stop_id, true);
                continue;
            }
        }
        if shared.final_stop_queued(agent_id) || lock(&shared.deferred_finals).contains(agent_id) {
            continue;
        }
        let Some(record) = records.get(agent_id) else {
            tracing::info!(agent = %agent_id, "remote node: stopping an agent head does not claim");
            shared.queue_final_stop(agent_id, StopId::new_v4(), false);
            continue;
        };
        if let Some(reason) = ended_reason(shared, record).await {
            tracing::info!(agent = %agent_id, reason = reason.as_str(), "remote node: stopping an agent whose session has ended");
            match crate::final_stop::record(&shared.store, record, reason).await {
                Ok(stop_id) => shared.queue_final_stop(agent_id, stop_id, true),
                Err(error) => {
                    tracing::warn!(agent = %agent_id, %error, "remote node: could not record a final stop; stopping it from memory");
                    shared.queue_final_stop(agent_id, StopId::new_v4(), false);
                }
            }
            continue;
        }
        // An agent with a plain stop queued is being stopped: parking a
        // runtime for it would hand a relaunch a runtime that the stopped
        // process's exit then ends (intent-hq/intent#8457).
        if !shared.agents.is_attached(agent_id) && !shared.stop_queued(agent_id) {
            let sink = shared.agents.attach(agent_id, record.workspace_id.clone());
            let runtime = Arc::new(RemoteAgentRuntime::new(
                agent_id.clone(),
                sink,
                shared.clone(),
            ));
            lock(&shared.parked).insert(agent_id.clone(), runtime);
        }
    }
    true
}

/// Why `record`'s agent ends for good, when its session has ended: its
/// workspace was retired, or it was retired or deleted. `None` while it may
/// still run, and when its session cannot be read (a read failure never
/// stops live work).
async fn ended_reason(
    shared: &Shared,
    record: &intent_core::remote_launch::RemoteAgentRecord,
) -> Option<intent_core::remote_launch::FinalStopReason> {
    use intent_core::remote_launch::FinalStopReason;
    let session = shared
        .store
        .get_agent_session_summary(&record.agent_id)
        .await
        .ok()?;
    if !crate::final_stop::session_ended(&session) {
        return None;
    }
    if session.status == intent_core::AgentStatus::Deleted {
        return Some(FinalStopReason::AgentDeleted);
    }
    let workspace_retired = shared
        .store
        .get_workspace(&record.workspace_id)
        .await
        .is_ok_and(|workspace| workspace.is_retired());
    Some(if workspace_retired {
        FinalStopReason::WorkspaceRetired
    } else {
        FinalStopReason::AgentRetired
    })
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
/// Hand one ingested record on: an agent's to its placement and sink. A
/// home's record is acknowledged and dropped while this backend holds no
/// homes.
async fn ingest_record(shared: &Shared, record: intent_core::node_link::JournalRecord) {
    let Some(agent_id) = record.agent_id().cloned() else {
        if let Some(inbound) = shared.inbound() {
            inbound.home_record(shared, record).await;
        }
        return;
    };
    shared.note_activity();
    record_placement(shared, &agent_id, &record).await;
    let exited = Shared::exited_agent(&agent_id, &record);
    shared.agents.deliver(&agent_id, record);
    shared.stop_exit_ingested(exited);
}

async fn record_placement(
    shared: &Shared,
    agent_id: &AgentId,
    record: &intent_core::node_link::JournalRecord,
) {
    if let JournalBody::Skills { skills } = &record.body {
        record_skills(shared, agent_id, record, skills.clone()).await;
        return;
    }
    let update: Box<dyn FnOnce(&mut intent_core::remote_launch::RemotePlacementView) + Send> =
        match &record.body {
            JournalBody::Started { .. } => {
                let machine = shared.machine_id();
                Box::new(move |view| {
                    view.effective_isolation = EffectiveIsolation::Isolated;
                    view.placement_error = None;
                    // Only for the machine the agent is placed on now: a
                    // record replayed by one it left never points back.
                    if !machine.is_empty() && view.node_id == machine {
                        view.launched_on = Some(machine);
                    }
                })
            }
            JournalBody::SetupStarted { started_at } => {
                let started_at = started_at.clone();
                Box::new(move |view| {
                    view.setup = Some(RemoteSetupStatus {
                        state: RemoteSetupState::Running,
                        started_at: Some(started_at),
                        ..RemoteSetupStatus::pending()
                    });
                })
            }
            JournalBody::SetupFinished {
                state,
                exit_code,
                error,
                finished_at,
            } => {
                let finished = RemoteSetupStatus {
                    state: match state {
                        SetupState::Completed => RemoteSetupState::Completed,
                        SetupState::Failed => RemoteSetupState::Failed,
                        SetupState::Skipped => RemoteSetupState::Skipped,
                    },
                    exit_code: *exit_code,
                    error: error.as_deref().map(RemoteSetupStatus::capped_error),
                    finished_at: Some(finished_at.clone()),
                    ..RemoteSetupStatus::pending()
                };
                Box::new(move |view| {
                    let started_at = view
                        .setup
                        .as_ref()
                        .filter(|setup| setup.state == RemoteSetupState::Running)
                        .and_then(|setup| setup.started_at.clone());
                    view.setup = Some(RemoteSetupStatus {
                        started_at,
                        ..finished
                    });
                })
            }
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
    match crate::view::update(&shared.store, &record.workspace_id, agent_id, update).await {
        // Announce the change so the agent list follows it (the list row
        // carries `effectiveIsolation` / `placementError`).
        Ok(true) => {
            if let Some(inbound) = &shared.inbound {
                inbound
                    .agent_placement_changed(&record.workspace_id, agent_id)
                    .await;
            }
        }
        Ok(false) => {}
        Err(error) => {
            tracing::warn!(agent = %agent_id, %error, "remote node: could not record the agent's placement state");
        }
    }
}

/// Keep the skills the machine reported for an agent with its placement
/// (not projected), and announce a change as `skills:changed` with the
/// agent.
async fn record_skills(
    shared: &Shared,
    agent_id: &AgentId,
    record: &intent_core::node_link::JournalRecord,
    skills: Vec<MachineSkill>,
) {
    match crate::view::update(&shared.store, &record.workspace_id, agent_id, move |view| {
        view.skills = Some(skills);
    })
    .await
    {
        Ok(true) => {
            if let Some(inbound) = &shared.inbound {
                inbound
                    .agent_skills_changed(&record.workspace_id, agent_id)
                    .await;
            }
        }
        Ok(false) => {}
        Err(error) => {
            tracing::warn!(agent = %agent_id, %error, "remote node: could not record the agent's skills");
        }
    }
}

/// Without the head's domain to answer with, refuse a node request.
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

/// The dials right after a resume. A tunnel restarted at the resume can stay
/// off its relay for good (intent#9895): when nothing answers, `revive`
/// restarts it, at most [`MAX_REVIVES`] times, and the machine is dialed
/// again. A revive that does nothing, or any failure but no answer, ends it.
async fn dial_with_revives<D, DF, R, RF>(
    mut dial: D,
    mut revive: R,
    machine: &str,
) -> std::result::Result<Connected, ConnectFailure>
where
    D: FnMut() -> DF,
    DF: std::future::Future<Output = std::result::Result<Connected, ConnectFailure>>,
    R: FnMut() -> RF,
    RF: std::future::Future<Output = bool>,
{
    let mut connected = dial().await;
    let mut revived = 0;
    while matches!(connected, Err(ConnectFailure::Unreachable(_))) && revived < MAX_REVIVES {
        if !revive().await {
            break;
        }
        revived += 1;
        tracing::info!(%machine, revived, "remote machine: no answer after the resume; its tunnel was restarted");
        connected = dial().await;
    }
    connected
}

#[cfg(test)]
mod revive_tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    async fn run(
        failures: impl Fn(u32) -> ConnectFailure,
        revives: impl Fn(u32) -> bool,
    ) -> (bool, u32, u32) {
        let dials = AtomicU32::new(0);
        let asked = AtomicU32::new(0);
        let out = dial_with_revives(
            || {
                let n = dials.fetch_add(1, Ordering::SeqCst);
                let failure = failures(n);
                async move { Err(failure) }
            },
            || {
                let n = asked.fetch_add(1, Ordering::SeqCst);
                let answer = revives(n);
                async move { answer }
            },
            "m",
        )
        .await;
        (
            matches!(out, Err(ConnectFailure::Unreachable(_))),
            dials.load(Ordering::SeqCst),
            asked.load(Ordering::SeqCst),
        )
    }

    #[tokio::test]
    async fn an_unanswered_machine_is_revived_at_most_twice() {
        let (unreachable, dials, revives) =
            run(|_| ConnectFailure::Unreachable(Vec::new()), |_| true).await;
        assert!(unreachable);
        assert_eq!((dials, revives), (MAX_REVIVES + 1, MAX_REVIVES));
    }

    #[tokio::test]
    async fn a_revive_that_did_nothing_ends_the_dials() {
        let (unreachable, dials, revives) =
            run(|_| ConnectFailure::Unreachable(Vec::new()), |_| false).await;
        assert!(unreachable);
        assert_eq!((dials, revives), (1, 1));
    }

    #[tokio::test]
    async fn only_no_answer_is_revived() {
        let (unreachable, dials, revives) =
            run(|_| ConnectFailure::Revoked(Vec::new()), |_| true).await;
        assert!(!unreachable);
        assert_eq!((dials, revives), (1, 0));
    }
}
