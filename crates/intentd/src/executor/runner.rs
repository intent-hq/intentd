//! Runs one backend's remote agents on this machine: admission against the
//! machine's shared agent limits, provider processes started with
//! `intent_acp::spawn_provider` (resolved like a local agent's) and driven
//! over the local ACP client, and a journal record for everything the
//! backend must see.
//!
//! Head learns about an agent only through the journal: provider
//! `session/update` notifications, turn ends and process exits (including a
//! launch that failed or was refused for capacity). Records of one agent are
//! journaled in order by a single per-agent pump.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use tokio::process::Child;
use tokio::sync::{mpsc, oneshot, watch};
use zeroize::Zeroizing;

use intent_acp::session::ActivityTracker;
use intent_acp::NpxLaunchDir;
use intent_acp::{
    ClientRequestHandler, Connection, ConnectionHooks, EventSink, FileService,
    IncomingNotification, IncomingRequest, PermissionPolicy, PermissionRegistry, ProfileEnv,
    SinkEvent, SpawnOptions,
};
use intent_core::node_link::{
    CheckoutOf, CheckoutRead, CheckoutReadOutcome, CheckoutReadReply, ExitFailure, FinalPush,
    ForgeFetchLaunch, ForgeLaunch, GitIdentity, JournalBody, JournalRecord, LaunchCommand,
    LeaseBinding, MachineCapacity, StopOutcome, SKILLS_PROTOCOL,
};
use intent_core::nodes::NodeCounter;
use intent_core::placement::CapacityRefusal;
use intent_core::remote_launch::workspace_api::MAX_LINK_ATTACHMENT_CHUNK_BYTES;
use intent_core::{AgentId, BoxFuture, WorkspaceId};
use intent_nodes::journal::{Journal, JournalEntry};
use intent_providers::ProviderConfig;

use super::checkout::{Checkouts, Prepared, FINAL_PUSH_TIMEOUT, WIP_PUSH_TIMEOUT};
use super::scripts::ScriptProcesses;
use super::setup::{self, SetupOutput, SetupStatus};
use super::skills;
use super::storage::ProviderSession;
use super::workspace_api::{self, HeadLink};

/// Error codes at the start of a journaled launch or turn error.
mod homes;
pub(crate) use homes::homes_ready;

pub(super) mod codes {
    /// The machine's shared agent limits (count or memory) deny the launch.
    pub const CAPACITY: &str = "node-capacity";
    /// The provider is not available on this machine.
    pub const PROVIDER: &str = "node-provider-unavailable";
    /// The provider process could not be started or set up.
    pub const START: &str = "node-agent-start-failed";
    /// A launch without a profile failed because the provider is not signed
    /// in on this machine.
    pub const SIGNED_OUT: &str = "node-provider-signed-out";
    /// The selected provider profile cannot be used on this node (a missing
    /// account folder, or an overlay the node cannot read). Nothing falls
    /// back to the node's own login.
    pub const PROFILE: &str = "node-profile-unavailable";
    /// The agent's checkout of its base commit could not be prepared.
    pub const CHECKOUT: &str = "node-checkout-failed";
    /// The agent is not running on this node.
    pub const NOT_RUNNING: &str = "node-agent-not-running";
    /// The agent already has a turn running.
    pub const BUSY: &str = "node-agent-busy";
    /// The turn failed.
    pub const TURN: &str = "node-turn-failed";
    /// The node is shutting down and starts no more agents.
    pub const SHUTTING_DOWN: &str = "node-shutting-down";
}

/// How long a stopped provider gets to exit after SIGTERM before SIGKILL.
const TERM_GRACE: Duration = Duration::from_secs(2);
/// How often a running provider's descendants are snapshotted, so helpers
/// that left its process group can still be found after it exits (its
/// children are reparented the moment it exits).
const TRACK_INTERVAL: Duration = Duration::from_secs(1);

/// Set in every provider's environment to a value unique to that run, so
/// its helpers (which inherit it) can be found after the provider exits,
/// wherever they were reparented to.
const RUN_MARKER_ENV: &str = "INTENT_NODE_RUN";

/// Where every agent of a backend that handed over its GitHub token finds
/// it (nodeProtocol 15).
pub(super) const GH_TOKEN_ENV: &str = "GH_TOKEN";
/// The whole of a runner's shutdown: its script runs end (bounded by
/// [`super::scripts::REAP_WAIT`]), then its agents are stopped, push their
/// last work (each push bounded by [`FINAL_PUSH_TIMEOUT`]), are cleaned up
/// and have their queued records (each one fsync'd) journaled, and every
/// final stop's cleanup under way finishes.
pub(super) const SHUTDOWN_WAIT: Duration = Duration::from_secs(75);

// A shutdown push gets its whole bound after the scripts are reaped and the
// agent is killed, all within the shutdown, and the shutdown ends well
// before the sitter force-kills a stopping daemon, leaving the rest of the
// daemon's stop room too, so a push is never killed midway
// (intent-hq/intent#8437).
const _: () = assert!(
    super::scripts::REAP_WAIT.as_secs() + TERM_GRACE.as_secs() + FINAL_PUSH_TIMEOUT.as_secs()
        < SHUTDOWN_WAIT.as_secs()
);
const _: () = assert!(
    SHUTDOWN_WAIT.as_secs() + 10 <= intentd_sitter::supervisor::DEFAULT_KILL_TIMEOUT.as_secs()
);

/// How this machine launches a provider: its registry entry, the executable
/// a local agent of it would spawn (or the npx package), and extra
/// environment.
#[derive(Debug, Clone)]
pub(crate) struct ProviderLaunch {
    pub config: ProviderConfig,
    pub binary: Option<PathBuf>,
    pub npx_fallback_binary: Option<PathBuf>,
    pub npx_fallback_package: Option<&'static str>,
    pub extra_env: BTreeMap<String, String>,
}

/// The machine's shared agent limits, which local agents count against
/// too. A launch they deny is refused, never queued.
pub(crate) trait Admission: Send + Sync {
    /// Take a slot for `agent_id`, or say which limit denies it.
    fn try_admit(&self, agent_id: &AgentId) -> Result<(), CapacityRefusal>;
    /// Give `agent_id`'s slot back.
    fn release(&self, agent_id: &AgentId);
    /// The limits and their use, for the capabilities report.
    fn capacity(&self) -> MachineCapacity;
}

/// What a launch reads from the machine's settings when it starts.
pub(crate) trait MachineLaunch: Send + Sync {
    /// Resolve `provider` as a local spawn would. The error is a short
    /// reason, never a path or setting value.
    fn resolve(&self, provider: &str) -> Result<ProviderLaunch, String>;
    /// `remoteAgents.permission`: how provider permission prompts are
    /// answered.
    fn policy(&self) -> PermissionPolicy;
    /// Whether this machine may hold workspace homes now: its
    /// `remoteAgents.allowHomes`, behind the build's readiness gate
    /// ([`homes_ready`]).
    fn allows_homes(&self) -> bool {
        homes_ready()
    }
}

/// Runs a quick action a backend sent (`oneShot`, nodeProtocol 18) with
/// this machine's own one-shot route.
pub(crate) trait MachineOneShot: Send + Sync {
    fn run(
        &self,
        frame: intent_core::node_link::OneShot,
        profile: Option<ProfileEnv>,
    ) -> intent_core::BoxFuture<'static, intent_core::node_link::OneShotReply>;
}

/// What the runner needs.
pub(super) struct RunnerConfig {
    /// Runs quick actions; `None` answers every `oneShot` as unavailable.
    pub one_shot: Option<Arc<dyn MachineOneShot>>,
    pub admission: Arc<dyn Admission>,
    pub launch: Arc<dyn MachineLaunch>,
    pub checkouts: Arc<dyn Checkouts>,
    pub journal: Journal,
    /// The node user's home: a leading `~` in a profile's account directory
    /// expands against it.
    pub home: Option<PathBuf>,
    /// The executable providers run as `mcp-bridge` to reach an agent's
    /// workspace MCP server.
    pub mcp_bridge_exe: PathBuf,
    /// The backend's helper socket, which `intentd git-credential --node`
    /// asks for the live forge token (nodeProtocol 15); `None` gives no
    /// agent the credential helper.
    pub git_socket: Option<PathBuf>,
    /// What a sub-agent's own forge helper socket needs (nodeProtocol 17);
    /// `None` gives no sub-agent a fetch path.
    pub forge_git: Option<ForgeGit>,
    /// Told the provider when a launch without a profile fails to
    /// authenticate.
    pub auth_failed: AuthFailed,
    /// Where folders other agents pull are packed (nodeProtocol 16).
    pub pull_staging: PathBuf,
}

/// The machine's git sessions and `git-remote-intent` directory, for the
/// forge helper socket of a sub-agent (nodeProtocol 17).
#[derive(Clone)]
pub(super) struct ForgeGit {
    pub helper_dir: PathBuf,
    pub sessions: intent_transport::node_link::NodeGitSessions,
}

/// A sub-agent's own forge helper socket, served until dropped.
struct ForgeSocket {
    path: PathBuf,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for ForgeSocket {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Told a provider id when that provider's default account (a launch
/// without a profile) failed to authenticate.
pub(crate) type AuthFailed = Arc<dyn Fn(&str) + Send + Sync>;

/// The node's agents. Cloning shares them.
#[derive(Clone)]
pub(super) struct Runner {
    inner: Arc<Inner>,
}

struct Inner {
    one_shot: Option<Arc<dyn MachineOneShot>>,
    /// Running quick actions, by request id, with the binding generation of
    /// the link that asked for them: dropping the sender ends one.
    one_shots: Mutex<
        HashMap<intent_core::node_link::LinkId, (NodeCounter, tokio::sync::oneshot::Sender<()>)>,
    >,
    admission: Arc<dyn Admission>,
    launch: Arc<dyn MachineLaunch>,
    checkouts: Arc<dyn Checkouts>,
    home: Option<PathBuf>,
    mcp_bridge_exe: PathBuf,
    git_socket: Option<PathBuf>,
    forge_git: Option<ForgeGit>,
    auth_failed: AuthFailed,
    /// The newest bound link; agents forward control-plane calls on it.
    head: HeadLink,
    journal: Arc<Mutex<Journal>>,
    appended: watch::Sender<NodeCounter>,
    state: Mutex<State>,
    /// Each agent's journal pump; shutdown waits for them so an agent's
    /// queued output and exit are durable before the node exits.
    pumps: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    /// The binding of the newest link session. Link sessions take it around
    /// adoption and every side effect, so a replaced session can act on
    /// nothing (see `session.rs`).
    link_gate: tokio::sync::Mutex<Option<LeaseBinding>>,
    /// Machine runs of the agents' saved scripts (nodeProtocol 8).
    scripts: ScriptProcesses,
    /// Runs the agents' setup scripts (nodeProtocol 10).
    pty: Arc<intent_pty::PtyHost>,
    /// Notified each time the bind sweep finishes removing one checkout.
    swept: tokio::sync::Notify,
    /// One push at a time per checkout, keyed by its owner: the owner and
    /// every agent sharing its checkout (nodeProtocol 13) push through it.
    push_locks: Mutex<HashMap<AgentId, std::sync::Weak<tokio::sync::Mutex<()>>>>,
    /// Set when shutdown begins: an ordinary push still under way is
    /// cancelled, so the agent's last push starts within the budget.
    closing_signal: watch::Sender<bool>,
    /// The backend's `headSession` and lease agents from the newest bound
    /// hello (nodeProtocol 14): a secret is held only for an agent on the
    /// lease, and a new session drops every secret.
    head_session: Mutex<HeadSession>,
    /// Serves pieces of the agents' checkout files to other agents'
    /// `ws.file.pull` (nodeProtocol 16).
    pulls: Arc<intent_services::checkout_transfer::PieceSource>,
    /// How long a provider may take to answer `initialize`.
    start_timeouts: Mutex<StartTimeouts>,
    /// The bound backend's workspace homes (nodeProtocol 19).
    homes: Mutex<homes::Homes>,
    /// Home opens run one at a time.
    home_opens: Arc<tokio::sync::Mutex<()>>,
}

/// The longest a provider's first start after a link binds may take to
/// answer `initialize`: the machine has often just woken, and its first
/// provider start loads it cold.
const COLD_INITIALIZE_TIMEOUT: Duration = Duration::from_secs(90);

/// See [`Inner::start_timeouts`].
struct StartTimeouts {
    warm: Duration,
    cold: Duration,
    /// A link bound and no provider has started since.
    cold_pending: bool,
}

impl Default for StartTimeouts {
    fn default() -> Self {
        let warm = intent_acp::initialize_timeout();
        Self {
            warm,
            cold: warm.max(COLD_INITIALIZE_TIMEOUT),
            cold_pending: false,
        }
    }
}

/// See [`Inner::head_session`].
#[derive(Default)]
struct HeadSession {
    session: Option<String>,
    lease_agents: HashSet<AgentId>,
    /// Agents that ended here for good (a final stop or the bind sweep): a
    /// `secretPut` still in flight for one is refused.
    ended: HashSet<AgentId>,
}

#[derive(Default)]
struct State {
    /// Shutdown began: no more agents start.
    closing: bool,
    /// Running (or starting) agents; their count is the capacity in use.
    agents: HashMap<AgentId, Arc<Agent>>,
    /// The workspace of every agent launched since start, so a prompt for an
    /// agent that has exited can still be answered with a journal record.
    workspaces: HashMap<AgentId, WorkspaceId>,
    /// Agents that left `agents` (their process tree is gone and their slot
    /// free) whose exit record is not journaled yet, so a stop arriving in
    /// that window still waits for the record.
    exiting: HashMap<AgentId, watch::Receiver<ExitRecord>>,
    /// Why each agent that left `agents` exited ([`exit_detail`]), for a
    /// prompt that arrives after it; cleared by its next launch or when it
    /// ends here for good.
    exit_details: HashMap<AgentId, String>,
    /// Agents a final stop is pushing and removing, each with the one
    /// cleanup every repeat of that stop joins (`None` until it finishes).
    /// The bind sweep leaves their checkouts to it, and a launch waits for
    /// it.
    finalizing: HashMap<AgentId, watch::Receiver<Option<FinalStopDone>>>,
    /// Agents launched since the newest bind sweep took its list: the sweep
    /// never removes their checkouts.
    launched: HashSet<AgentId>,
    /// Agents whose checkout the bind sweep is removing now; their launch
    /// waits for it.
    sweeping: HashSet<AgentId>,
    /// Every agent launched since start into another agent's checkout
    /// (nodeProtocol 13), with that checkout's owner.
    shares: HashMap<AgentId, AgentId>,
}

impl State {
    /// The owner of the checkout `agent_id` works in: its own, unless it
    /// shares one.
    fn checkout_owner(&self, agent_id: &AgentId) -> AgentId {
        self.shares
            .get(agent_id)
            .cloned()
            .unwrap_or_else(|| agent_id.clone())
    }

    /// The agents that work in `owner`'s checkout and are running or
    /// exiting.
    fn live_sharers(&self, owner: &AgentId) -> Vec<AgentId> {
        self.shares
            .iter()
            .filter(|(agent_id, of)| {
                *of == owner
                    && (self.agents.contains_key(*agent_id) || self.exiting.contains_key(*agent_id))
            })
            .map(|(agent_id, _)| agent_id.clone())
            .collect()
    }
}

/// A final stop's answer: the acknowledgement's outcome and push report, or
/// `None` when it may not be acknowledged.
type FinalStopDone = Option<(StopOutcome, FinalPush)>;

/// Finishes a final stop's cleanup when its task ends, normally or not:
/// drops its `finalizing` entry, publishes its answer (`None`, so the stop
/// is not acknowledged and the backend resends it, unless the cleanup set
/// one) and wakes launches waiting on it.
struct FinalizingGuard {
    runner: Runner,
    agent_id: AgentId,
    finished: watch::Sender<Option<FinalStopDone>>,
    result: FinalStopDone,
}

impl Drop for FinalizingGuard {
    fn drop(&mut self) {
        {
            let mut state = lock(&self.runner.inner.state);
            if state
                .finalizing
                .get(&self.agent_id)
                .is_some_and(|done| done.same_channel(&self.finished.subscribe()))
            {
                state.finalizing.remove(&self.agent_id);
            }
        }
        self.finished.send_replace(Some(self.result));
        // A launch waiting for this cleanup goes ahead.
        self.runner.inner.swept.notify_waiters();
    }
}

/// Whether an agent's exit record reached the journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ExitRecord {
    /// Not appended yet.
    Pending,
    Journaled,
    /// The append failed, or the pump ended without one: it never will be.
    Lost,
}

/// What [`Runner::stop`] leaves to wait for before the stop may be
/// acknowledged.
pub(super) enum StopWait {
    /// No agent with that id runs or is exiting.
    NotRunning,
    /// Resolves with `outcome` once the agent's exit record is journaled.
    Pending {
        outcome: StopOutcome,
        journaled: watch::Receiver<ExitRecord>,
    },
}

impl StopWait {
    /// Wait until the stop is applied: the agent's process tree is gone, its
    /// shared slot is free and its exit record is journaled. `None` when the
    /// exit record could not be journaled: the stop must not be
    /// acknowledged (the backend resends it, and the resend finds the agent
    /// not running).
    pub(super) async fn wait(self) -> Option<StopOutcome> {
        match self {
            Self::NotRunning => Some(StopOutcome::NotRunning),
            Self::Pending {
                outcome,
                mut journaled,
            } => match journaled
                .wait_for(|state| *state != ExitRecord::Pending)
                .await
                .map(|state| *state)
            {
                Ok(ExitRecord::Journaled) => Some(outcome),
                // A closed channel without a settled state: the pump is
                // gone and never journaled the exit.
                Ok(ExitRecord::Pending | ExitRecord::Lost) | Err(_) => None,
            },
        }
    }
}

/// A journal entry of `agent` with `body`, outside any turn.
fn agent_entry(agent: &Agent, body: JournalBody) -> JournalEntry {
    JournalEntry {
        owner: agent.id.clone().into(),
        workspace_id: agent.workspace_id.clone(),
        turn_id: None,
        recorded_at: intent_core::now_iso(),
        body,
    }
}

/// What the checkout's setup left for the agent's start.
#[derive(Default)]
struct Settled {
    /// Prepended to the first turn: setup did not complete.
    notice: Option<String>,
    /// The `setupStatus` its `workspace.details` shows.
    overlay: Option<Value>,
    /// The agent was stopped during setup: it does not start.
    stopped: bool,
}

/// Add to `settled` the submodules populated in the checkout at `cwd` that
/// are not among its recorded pins (the setup script or an earlier session
/// initialized them): the first turn's notice names them and the agent's
/// `setupStatus` lists them as `unsentSubmodules`.
async fn unsent_submodules(settled: &mut Settled, cwd: &Path) {
    let cwd = cwd.to_path_buf();
    let found = tokio::task::spawn_blocking(move || {
        let pins = super::checkout::recorded_pins(&cwd);
        intent_git::node_git::unpinned_submodules(&cwd, &pins)
    })
    .await;
    let paths = match found {
        Ok(Ok(paths)) => paths,
        Ok(Err(error)) => {
            tracing::warn!(%error, "node: could not list the checkout's unpinned submodules");
            return;
        }
        Err(error) => {
            tracing::warn!(%error, "node: the unpinned-submodule check did not finish");
            return;
        }
    };
    let Some(notice) = setup::unsent_notice(&paths) else {
        return;
    };
    settled.notice = Some(match settled.notice.take() {
        Some(setup) => format!("{setup}\n\n{notice}"),
        None => notice,
    });
    if let Some(Value::Object(overlay)) = settled.overlay.as_mut() {
        overlay.insert("unsentSubmodules".to_owned(), serde_json::json!(paths));
    }
}

struct Agent {
    id: AgentId,
    workspace_id: WorkspaceId,
    /// The launch's provider id and model, for the memory report.
    provider: String,
    model: Option<String>,
    /// The launch's display labels, for the memory report only.
    labels: intent_core::node_link::LaunchLabels,
    /// The provider process's pid while it runs, for the memory sampler.
    root_pid: Mutex<Option<u32>>,
    /// The commit head launched the agent at, and the checkout cache key.
    base_commit: String,
    repo_key: String,
    /// The submodules its checkout has at their pins (nodeProtocol 10).
    submodules: Vec<intent_core::node_link::SubmodulePin>,
    /// Whether the launch carried a prepared checkout (nodeProtocol 10).
    prepared: bool,
    /// Its setup script, if any.
    setup: Option<intent_core::node_link::SetupCommand>,
    /// The checkout's progress and setup output while they run, resent
    /// after a reconnect.
    setup_output: Mutex<Option<Arc<SetupOutput>>>,
    /// The session's `agentFeatures` snapshot and sub-agent flag, which gate
    /// its workspace API.
    gates: workspace_api::Gates,
    /// The agent's working directory, once prepared.
    worktree: Mutex<Option<PathBuf>>,
    /// Prepended to the agent's first turn (see [`Prepared::notice`]).
    notice: Mutex<Option<String>>,
    /// The machine's name, when head says the agent's last run was on
    /// another machine: its first turn's notice says so.
    new_machine: Option<String>,
    /// Head says the machine resumed from its disk since this agent last
    /// ran here: its first turn's notice says what was kept and lost.
    resumed_from_disk: bool,
    /// Whether this launch still tries to load the recorded provider
    /// session (a disk resume): once only, so the start retried after a
    /// provider that died loading it opens a new session.
    load_recorded: std::sync::atomic::AtomicBool,
    /// The machine's skills catalog block, added to the agent's first turn.
    skills_catalog: Mutex<Option<String>>,
    /// The owner of the checkout the agent works in, when it is not its own
    /// (nodeProtocol 13): its pushes go to the owner's refs.
    checkout_of: Option<AgentId>,
    /// Its workspace branch and GitHub remote (nodeProtocol 15): top-level
    /// agents only.
    forge: Option<ForgeLaunch>,
    /// Its GitHub repository, fetched through the backend (nodeProtocol
    /// 17): sub-agents only.
    forge_fetch: Option<ForgeFetchLaunch>,
    /// The backend's commit identity for the workspace, set as the
    /// provider's `GIT_AUTHOR_*` / `GIT_COMMITTER_*`.
    git_identity: Option<intent_core::node_link::GitIdentity>,
    /// Serializes the pushes of the agent's checkout, shared with its owner
    /// and every agent sharing it.
    wip: Arc<tokio::sync::Mutex<()>>,
    /// A final stop ends the agent: its exit push is bounded by
    /// [`FINAL_PUSH_TIMEOUT`] and reported in `final_push`.
    final_stop: std::sync::atomic::AtomicBool,
    final_push: Mutex<Option<FinalPush>>,
    stop: Mutex<Option<oneshot::Sender<()>>>,
    /// Whether the agent's exit record reached the journal.
    journaled: watch::Receiver<ExitRecord>,
    ready: watch::Receiver<Option<Arc<Ready>>>,
    /// Why the agent exited ([`exit_detail`]), set before `ready` closes so
    /// a turn waiting for the session can say why it never came.
    exit_detail: Mutex<Option<String>>,
    /// Whether a turn is running; refuses a second one.
    busy: Mutex<bool>,
    marks: mpsc::UnboundedSender<Mark>,
    activity: ActivityTracker,
    /// The provider's descendants at the latest snapshot.
    descendants: Arc<Descendants>,
    /// This run's [`RUN_MARKER_ENV`] value.
    run_marker: String,
    /// Scrubs the profile's secret values from what the agent journals.
    redactor: Arc<Redactor>,
    /// An npx launch's working directory, kept while the agent runs.
    npx_launch_dir: Mutex<Option<NpxLaunchDir>>,
    /// For a launch without a profile: its provider and whom to tell when
    /// the provider's default account fails to authenticate. A profile's
    /// account says nothing about the default one.
    default_account: Option<(String, AuthFailed)>,
    /// The forge token value its current process was spawned with
    /// (nodeProtocol 15), masked machine-wide while held.
    forge_lease: Mutex<Option<super::forge::SpawnLease>>,
}

impl Agent {
    /// The agent whose refs this agent's pushes update.
    fn checkout_owner(&self) -> &AgentId {
        self.checkout_of.as_ref().unwrap_or(&self.id)
    }

    /// Report a setup or turn failure that is the provider refusing the
    /// default account as not signed in.
    fn note_auth_failure(&self, error: &intent_acp::AcpError) {
        if let Some((provider, auth_failed)) = &self.default_account {
            if intent_services::provider_auth::is_auth_required(error) {
                auth_failed(provider);
            }
        }
    }
}

/// Replaces a launch profile's secret values with [`Masks::MASK`] in the
/// provider-originated text the node journals or logs (session updates and
/// provider error messages, which can echo a rejected credential). Every
/// value of a credential-named variable (`*_API_KEY`, `*_TOKEN`, `*SECRET*`,
/// `*PASSWORD*`, ...; [`intent_acp::is_likely_secret_env_key`]) is masked
/// whatever its length; another variable's value is masked from
/// [`Masks::MIN_LEN`] bytes, so a short setting such as `1` does not
/// scrub ordinary text. Account-folder paths are not secrets.
///
/// It also masks every GitHub token the agent was spawned with or the
/// machine held while it ran (nodeProtocol 15), whatever its length.
#[derive(Default)]
struct Redactor {
    profile: Vec<Zeroizing<String>>,
    /// The machine's forge token values (nodeProtocol 15): the held one and
    /// every live agent's, whatever this agent was spawned with.
    forge: Option<Arc<super::forge::ForgeToken>>,
    /// The masks, with the forge version they were built at.
    masks: Mutex<(u64, Arc<Masks>)>,
}

impl Redactor {
    #[cfg(test)]
    fn new(profile: Option<&ProfileEnv>) -> Self {
        Self::with_forge(profile, None)
    }

    fn with_forge(
        profile: Option<&ProfileEnv>,
        forge: Option<Arc<super::forge::ForgeToken>>,
    ) -> Self {
        let masks = Masks::new(profile);
        Self {
            profile: masks.secrets.clone(),
            forge,
            masks: Mutex::new((u64::MAX, Arc::new(masks))),
        }
    }

    fn masks(&self) -> Arc<Masks> {
        let mut cached = lock(&self.masks);
        let Some(forge) = &self.forge else {
            return cached.1.clone();
        };
        let version = forge.version();
        if cached.0 != version {
            let mut secrets = self.profile.clone();
            secrets.extend(forge.masked());
            secrets.sort_by_key(|secret| std::cmp::Reverse(secret.len()));
            secrets.dedup();
            *cached = (version, Arc::new(Masks { secrets }));
        }
        cached.1.clone()
    }

    fn text(&self, text: String) -> String {
        self.masks().text(text)
    }

    #[cfg(test)]
    fn value(&self, value: &mut Value) {
        self.masks().value(value);
    }

    fn notification(&self, params: &mut Value) {
        self.masks().notification(params);
    }
}

/// One snapshot of the values a [`Redactor`] masks.
#[derive(Default)]
struct Masks {
    /// Longest first, so a secret containing another is masked whole.
    secrets: Vec<Zeroizing<String>>,
}

impl Masks {
    const MASK: &'static str = "[redacted]";
    const MIN_LEN: usize = 8;

    fn new(profile: Option<&ProfileEnv>) -> Self {
        let mut secrets: Vec<Zeroizing<String>> = profile
            .into_iter()
            .flat_map(ProfileEnv::entries)
            .filter(|(key, _)| {
                !intent_acp::profile_env::ACCOUNT_DIR_KEYS
                    .iter()
                    .any(|dir| dir.eq_ignore_ascii_case(key))
            })
            .filter_map(|(key, value)| {
                let value = value.filter(|value| !value.is_empty())?;
                (intent_acp::is_likely_secret_env_key(key) || value.len() >= Self::MIN_LEN)
                    .then(|| Zeroizing::new(value.to_owned()))
            })
            .collect();
        secrets.sort_by_key(|secret| std::cmp::Reverse(secret.len()));
        secrets.dedup();
        Self { secrets }
    }

    fn text(&self, text: String) -> String {
        if !self
            .secrets
            .iter()
            .any(|secret| text.contains(secret.as_str()))
        {
            return text;
        }
        self.secrets.iter().fold(text, |text, secret| {
            text.replace(secret.as_str(), Self::MASK)
        })
    }

    /// Fields whose content is free-form provider data, not ACP schema:
    /// their object keys are data too and are masked.
    const FREE_FORM: [&'static str; 3] = ["rawInput", "rawOutput", "_meta"];

    /// Mask secrets in provider data: every string value. Object keys are
    /// kept, since outside [`Self::FREE_FORM`] they are ACP field names
    /// (an embedded resource's `text`, a location's `path`) that head needs
    /// to parse the update; inside a free-form field the keys are masked too.
    fn value(&self, value: &mut Value) {
        if self.secrets.is_empty() {
            return;
        }
        match value {
            Value::String(text) => *text = self.text(std::mem::take(text)),
            Value::Array(items) => items.iter_mut().for_each(|item| self.value(item)),
            Value::Object(map) => {
                for (key, item) in map.iter_mut() {
                    if Self::FREE_FORM.contains(&key.as_str()) {
                        self.data(item);
                    } else {
                        self.value(item);
                    }
                }
            }
            _ => {}
        }
    }

    /// Mask secrets in free-form data (a tool's raw input or output, `_meta`):
    /// every string and every object key.
    fn data(&self, value: &mut Value) {
        if self.secrets.is_empty() {
            return;
        }
        match value {
            Value::String(text) => *text = self.text(std::mem::take(text)),
            Value::Array(items) => items.iter_mut().for_each(|item| self.data(item)),
            Value::Object(map) => {
                let entries = std::mem::take(map);
                *map = entries
                    .into_iter()
                    .map(|(key, mut item)| {
                        self.data(&mut item);
                        (self.text(key), item)
                    })
                    .collect();
            }
            _ => {}
        }
    }

    /// Mask a `session/update` notification's params. Field names and the
    /// string identifiers at their ACP schema positions (the session id,
    /// the update's variant tag, tool kind/status/id, content block and
    /// tool-content tags, an annotation's `audience` roles, a config option's
    /// `type`, plan entry status/priority) are kept as sent, so head can
    /// still parse the update even when a short secret equals a protocol
    /// word; every other string is masked with [`Self::value`], and tool
    /// `rawInput` / `rawOutput` / `_meta` with [`Self::data`] (keys too).
    ///
    /// These are every closed enum the typed `SessionUpdate` parse reaches;
    /// the remaining enums there (a notice's severity, a config option's
    /// category) accept any string, so masking them never breaks the parse.
    fn notification(&self, params: &mut Value) {
        if self.secrets.is_empty() {
            return;
        }
        self.schema(params, &|key, _| match key {
            "sessionId" => Keep::Id,
            "update" => Keep::Walk(Self::update),
            _ => Keep::Mask,
        });
    }

    fn update(&self, update: &mut Value) {
        self.schema(update, &|key, _| match key {
            "sessionUpdate" | "kind" | "status" | "toolCallId" => Keep::Id,
            "content" => Keep::Walk(Self::content),
            "entries" => Keep::Walk(Self::plan_entries),
            "configOptions" => Keep::Walk(Self::config_options),
            _ => Keep::Mask,
        });
    }

    /// A content block's annotations: the `audience` roles are an enum.
    fn annotations(&self, annotations: &mut Value) {
        self.schema(annotations, &|key, _| match key {
            "audience" => Keep::Walk(Self::roles),
            _ => Keep::Mask,
        });
    }

    /// An annotation's audience: each role is kept only when it is one of
    /// ACP's role words; anything else is masked as data.
    fn roles(&self, roles: &mut Value) {
        let Value::Array(items) = roles else {
            return self.value(roles);
        };
        for item in items {
            self.enum_word(item, &["assistant", "user"]);
        }
    }

    /// A config option update's options: each option's `type` tags its kind.
    fn config_options(&self, options: &mut Value) {
        let Value::Array(items) = options else {
            return self.value(options);
        };
        for item in items {
            self.schema(item, &|key, _| match key {
                "type" => Keep::Walk(Self::config_kind),
                _ => Keep::Mask,
            });
        }
    }

    fn config_kind(&self, kind: &mut Value) {
        self.enum_word(kind, &["boolean", "select"]);
    }

    /// Keep `value` as sent when it is exactly one of the enum's `words`, so
    /// the update still parses; mask it as data otherwise, so nothing but a
    /// protocol word ever passes unmasked.
    fn enum_word(&self, value: &mut Value, words: &[&str]) {
        if !value.as_str().is_some_and(|word| words.contains(&word)) {
            self.value(value);
        }
    }

    /// A content block, a tool call's content list, or a tool content item
    /// (`{ type, content: <block> }`).
    fn content(&self, content: &mut Value) {
        if let Value::Array(items) = content {
            for item in items {
                self.content(item);
            }
            return;
        }
        self.schema(content, &|key, _| match key {
            "type" | "mimeType" => Keep::Id,
            "content" => Keep::Walk(Self::content),
            "annotations" => Keep::Walk(Self::annotations),
            _ => Keep::Mask,
        });
    }

    fn plan_entries(&self, entries: &mut Value) {
        let Value::Array(items) = entries else {
            return self.value(entries);
        };
        for item in items {
            self.schema(item, &|key, _| match key {
                "status" | "priority" => Keep::Id,
                _ => Keep::Mask,
            });
        }
    }

    /// Walk one schema object: keys stay, and each field is kept (a string
    /// identifier only), walked with its own schema, or masked as data. A
    /// value that is not an object is masked as data.
    fn schema(&self, value: &mut Value, field: &dyn Fn(&str, &Value) -> Keep) {
        let Value::Object(map) = value else {
            return self.value(value);
        };
        for (key, item) in map.iter_mut() {
            match field(key, item) {
                Keep::Id if item.is_string() => {}
                Keep::Walk(walk) => walk(self, item),
                Keep::Id | Keep::Mask if Self::FREE_FORM.contains(&key.as_str()) => {
                    self.data(item);
                }
                Keep::Id | Keep::Mask => self.value(item),
            }
        }
    }
}

/// The provider's descendants at the latest snapshot. Snapshots are taken
/// by the periodic tracker and once after setup, concurrently.
#[derive(Default)]
struct Descendants {
    started: std::sync::atomic::AtomicU64,
    latest: Mutex<(u64, Vec<i32>)>,
}

impl Descendants {
    /// Start a snapshot: its order among the snapshots.
    fn begin(&self) -> u64 {
        self.started
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1
    }

    /// Publish snapshot `scan`'s descendants, unless a snapshot that
    /// started later is already published: an older periodic scan finishing
    /// after the setup scan must not erase a helper the setup scan captured.
    /// Snapshots are never accumulated (a remembered pid can be reused).
    fn publish(&self, scan: u64, pids: Vec<i32>) {
        let mut latest = lock(&self.latest);
        if scan > latest.0 {
            *latest = (scan, pids);
        }
    }

    /// The descendants of the latest published snapshot.
    fn pids(&self) -> Vec<i32> {
        lock(&self.latest).1.clone()
    }
}

/// What [`Masks::schema`] does with one field.
enum Keep {
    /// A protocol identifier: kept as sent when it is a string.
    Id,
    /// A nested schema object.
    Walk(fn(&Masks, &mut Value)),
    /// Arbitrary data: masked.
    Mask,
}

/// A provider session that finished its ACP setup.
struct Ready {
    connection: Arc<Connection>,
    session_id: String,
}

/// What the agent's pump handles after every notification already
/// received.
enum Mark {
    /// The checkout and provider session are ready.
    Started { base_commit: String },
    /// Tag notifications with `turn_id` from now on; acknowledged once every
    /// earlier notification is journaled, before the prompt is sent.
    TurnStarted {
        turn_id: String,
        ack: oneshot::Sender<()>,
    },
    TurnEnded {
        turn_id: String,
        stop_reason: String,
        error: Option<String>,
    },
    /// A `session/load` begins: drop the history it replays from now on.
    /// Acknowledged before the load is sent.
    Replaying { ack: oneshot::Sender<()> },
    /// The load answered: drop what it still replays until the provider
    /// is quiet, then acknowledge.
    Replayed { ack: Option<oneshot::Sender<()>> },
    Exited {
        exit_code: Option<i32>,
        error: Option<String>,
    },
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Provider callbacks have no client on the node; events are only logged.
struct NodeSink;

impl EventSink for NodeSink {
    fn publish(&self, event: SinkEvent) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            tracing::debug!(event_type = %event.event_type, "node: provider event");
        })
    }
}

impl Runner {
    pub(super) fn new(config: RunnerConfig) -> Self {
        let last = config.journal.last_seq();
        let pty = Arc::new(intent_pty::PtyHost::new());
        let pulls = Arc::new(intent_services::checkout_transfer::PieceSource::new(
            config.pull_staging,
        ));
        intent_services::checkout_transfer::PieceSource::spawn_sweeper(&pulls);
        Self {
            inner: Arc::new(Inner {
                one_shot: config.one_shot,
                one_shots: Mutex::new(HashMap::new()),
                admission: config.admission,
                launch: config.launch,
                checkouts: config.checkouts,
                home: config.home,
                mcp_bridge_exe: config.mcp_bridge_exe,
                git_socket: config.git_socket,
                forge_git: config.forge_git,
                auth_failed: config.auth_failed,
                head: HeadLink::default(),
                journal: Arc::new(Mutex::new(config.journal)),
                appended: watch::channel(last).0,
                state: Mutex::new(State::default()),
                pumps: Mutex::new(Vec::new()),
                link_gate: tokio::sync::Mutex::new(None),
                scripts: ScriptProcesses::new(pty.clone()),
                pty,
                swept: tokio::sync::Notify::new(),
                push_locks: Mutex::new(HashMap::new()),
                closing_signal: watch::channel(false).0,
                head_session: Mutex::new(HeadSession::default()),
                pulls,
                start_timeouts: Mutex::new(StartTimeouts::default()),
                homes: Mutex::new(homes::Homes::default()),
                home_opens: Arc::new(tokio::sync::Mutex::new(())),
            }),
        }
    }

    /// Run a backend's quick action (`oneShot`, nodeProtocol 18) and return
    /// its reply; `oneShotCancel` with the same id ends it as `cancelled`.
    /// Register a quick action before its task runs, so a cancel or the
    /// end of its link always finds it; pass the receiver to
    /// [`Self::one_shot`].
    pub(super) fn begin_one_shot(
        &self,
        id: intent_core::node_link::LinkId,
        generation: NodeCounter,
    ) -> tokio::sync::oneshot::Receiver<()> {
        let (cancel, cancelled) = tokio::sync::oneshot::channel();
        lock(&self.inner.one_shots).insert(id, (generation, cancel));
        cancelled
    }

    pub(super) async fn one_shot(
        &self,
        frame: intent_core::node_link::OneShot,
        cancelled: tokio::sync::oneshot::Receiver<()>,
    ) -> intent_core::node_link::OneShotReply {
        use intent_core::node_link::{OneShotOutcome, OneShotReply};
        let id = frame.request_id;
        let Some(runs) = self.inner.one_shot.clone() else {
            lock(&self.inner.one_shots).remove(&id);
            return OneShotReply::refused(
                id,
                OneShotOutcome::Unavailable,
                "gate-closed",
                "quick actions do not run on this machine",
            );
        };
        let profile = match frame
            .profile_env
            .as_ref()
            .map(|wire| self.node_profile(&wire.0))
        {
            None => None,
            Some(Ok(profile)) => Some(profile),
            Some(Err(error)) => {
                lock(&self.inner.one_shots).remove(&id);
                return OneShotReply::refused(id, OneShotOutcome::Failed, "adapter-failed", error);
            }
        };
        let reply = tokio::select! {
            reply = runs.run(frame, profile) => reply,
            _ = cancelled => OneShotReply {
                request_id: id,
                outcome: OneShotOutcome::Cancelled,
                text: None,
                error: None,
            },
        };
        lock(&self.inner.one_shots).remove(&id);
        reply
    }

    /// End every running quick action that link generation `generation`
    /// asked for: that link is gone, so no one waits for the replies
    /// (node-link.md "Quick actions on the machine", rule 2). Quick actions
    /// of a newer link of the same backend keep running.
    pub(super) fn cancel_binding_one_shots(&self, generation: NodeCounter) {
        let mut runs = lock(&self.inner.one_shots);
        let ended: Vec<_> = runs
            .iter()
            .filter(|(_, (owner, _))| *owner == generation)
            .map(|(id, _)| *id)
            .collect();
        for id in ended {
            if let Some((_, cancel)) = runs.remove(&id) {
                let _ = cancel.send(());
            }
        }
    }

    /// End a running quick action (`oneShotCancel`); an unknown or finished
    /// id is ignored.
    pub(super) fn cancel_one_shot(&self, id: intent_core::node_link::LinkId) {
        if let Some((_, cancel)) = lock(&self.inner.one_shots).remove(&id) {
            let _ = cancel.send(());
        }
    }

    /// Adopt a bound hello's `headSession` and lease agents (nodeProtocol
    /// 14). A different session, or none, means another backend process:
    /// every secret this backend's agents held here is dropped.
    pub(super) fn adopt_head(&self, session: Option<&str>, lease_agents: &[AgentId]) {
        let mut head = lock(&self.inner.head_session);
        if session.is_none() || head.session.as_deref() != session {
            self.inner.head.secrets().clear();
            self.inner.head.forge().clear();
        }
        head.session = session.map(str::to_owned);
        head.lease_agents = lease_agents.iter().cloned().collect();
        drop(head);
        lock(&self.inner.start_timeouts).cold_pending = true;
    }

    /// The `initialize` timeout for a provider starting now: the cold one
    /// for the first start since a link bound, never for a second start.
    fn initialize_timeout(&self, second: bool) -> Duration {
        let mut timeouts = lock(&self.inner.start_timeouts);
        if !second && std::mem::take(&mut timeouts.cold_pending) {
            timeouts.cold
        } else {
            timeouts.warm
        }
    }

    /// Replace the `initialize` timeouts.
    #[cfg(test)]
    pub(super) fn set_initialize_timeouts(&self, warm: Duration, cold: Duration) {
        let mut timeouts = lock(&self.inner.start_timeouts);
        timeouts.warm = warm;
        timeouts.cold = cold;
    }

    /// `agent_id` ended here for good: drop its secrets and refuse any
    /// later `secretPut` for it.
    fn end_secrets(&self, agent_id: &AgentId) {
        let mut head = lock(&self.inner.head_session);
        head.ended.insert(agent_id.clone());
        head.lease_agents.remove(agent_id);
        drop(head);
        lock(&self.inner.state).exit_details.remove(agent_id);
        self.inner.head.secrets().forget(agent_id, None);
        self.inner.pulls.forget(agent_id.as_str());
    }

    /// Apply a `forgeToken` frame (nodeProtocol 15): hold its token, or drop
    /// the held one. Running agents' journals mask it from now on; they get
    /// it in `GH_TOKEN` only at their next spawn. Never logs or returns the
    /// value.
    pub(super) fn forge_frame(
        &self,
        frame: intent_core::node_link::ForgeToken,
    ) -> intent_core::node_link::ForgeTokenReply {
        use intent_core::node_link::{
            ForgeTokenReply, SecretOutcome, FORGE_HOST_GITHUB, FORGE_TOKEN_INVALID,
        };
        let request_id = frame.request_id;
        let usable = frame.host == FORGE_HOST_GITHUB
            && frame
                .token
                .as_ref()
                .is_none_or(|token| super::forge::ForgeToken::usable(token.expose()));
        if !usable {
            return ForgeTokenReply {
                request_id,
                outcome: SecretOutcome::Failed,
                error: Some(FORGE_TOKEN_INVALID.to_owned()),
            };
        }
        let token = frame
            .token
            .map(intent_core::node_link::SecretValue::into_inner);
        self.inner.head.forge().set(token);
        ForgeTokenReply {
            request_id,
            outcome: SecretOutcome::Ok,
            error: None,
        }
    }

    /// Drop the backend's GitHub token: a link that negotiated less than
    /// nodeProtocol 15 never hands one over.
    pub(super) fn drop_forge(&self) {
        self.inner.head.forge().clear();
    }

    /// The backend's held GitHub token, which the credential helper socket
    /// serves (nodeProtocol 15).
    pub(super) fn forge_token(&self) -> Arc<super::forge::ForgeToken> {
        self.inner.head.forge().clone()
    }

    /// Apply one secret frame and build its reply; `None` for any other
    /// frame. Never logs or returns a value.
    pub(super) fn secret_frame(
        &self,
        frame: intent_core::node_link::LinkFrame,
    ) -> Option<intent_core::node_link::SecretReply> {
        use intent_core::node_link::{
            LinkFrame, SecretOutcome, SecretReply, SECRET_AGENT_NOT_ON_LEASE, SECRET_INVALID_PARAMS,
        };
        use intent_services::secret_requests::HeldSecrets;
        let (request_id, agent_id) = match &frame {
            LinkFrame::SecretPut(put) => (put.request_id, put.agent_id.clone()),
            LinkFrame::SecretForget(forget) => (forget.request_id, forget.agent_id.clone()),
            LinkFrame::SecretList(list) => (list.request_id, list.agent_id.clone()),
            _ => return None,
        };
        let failed = |error: &str| SecretReply {
            request_id,
            agent_id: agent_id.clone(),
            outcome: SecretOutcome::Failed,
            names: None,
            forgotten: None,
            error: Some(error.to_owned()),
        };
        let mut ok = SecretReply {
            outcome: SecretOutcome::Ok,
            error: None,
            ..failed("")
        };
        let on_lease = {
            let head = lock(&self.inner.head_session);
            !head.ended.contains(&agent_id)
                && (head.lease_agents.contains(&agent_id)
                    || lock(&self.inner.state).workspaces.contains_key(&agent_id))
        };
        let secrets = self.inner.head.secrets();
        Some(match frame {
            LinkFrame::SecretPut(_) if !on_lease => failed(SECRET_AGENT_NOT_ON_LEASE),
            LinkFrame::SecretPut(put) => {
                if HeldSecrets::validate(&put.name, put.value.expose()).is_err() {
                    failed(SECRET_INVALID_PARAMS)
                } else {
                    secrets.put(&agent_id, put.name, put.value.into_inner());
                    ok
                }
            }
            LinkFrame::SecretForget(forget) => {
                ok.forgotten = Some(secrets.forget(&agent_id, forget.name.as_deref()));
                ok
            }
            LinkFrame::SecretList(_) => {
                ok.names = Some(secrets.names(&agent_id));
                ok
            }
            _ => return None,
        })
    }

    /// Start one process of a machine run for the link binding
    /// `generation`, in the agent's checkout when it has one here.
    pub(super) async fn script_start(
        &self,
        link: &intent_transport::node_link::NodeLink,
        generation: NodeCounter,
        command: intent_core::node_link::ScriptStartCommand,
    ) {
        let owner = lock(&self.inner.state).checkout_owner(&command.agent_id);
        let checkout = self.inner.checkouts.existing(&owner);
        self.inner
            .scripts
            .start(link, generation, checkout.as_deref(), command)
            .await;
    }

    /// Stop one script process; its `scriptExited` acknowledges the stop.
    pub(super) fn script_stop(
        &self,
        link: &intent_transport::node_link::NodeLink,
        process: &intent_core::node_link::ScriptProcessRef,
    ) {
        self.inner.scripts.stop(link, process);
    }

    /// Kill every script process the link binding `generation` started:
    /// its link ended, so no script outlives it.
    pub(super) async fn end_binding_scripts(&self, generation: NodeCounter) {
        self.inner.scripts.stop_binding(generation).await;
    }

    /// The machine's shared limits and their use.
    #[cfg(test)]
    pub(super) fn capacity(&self) -> MachineCapacity {
        self.inner.admission.capacity()
    }

    /// The agents running or starting, for the welcome.
    pub(super) fn running(&self) -> Vec<AgentId> {
        let mut ids: Vec<AgentId> = lock(&self.inner.state).agents.keys().cloned().collect();
        ids.sort_by(|a, b| a.0.cmp(&b.0));
        ids
    }

    /// Every running agent's provider process pid with what its launch
    /// said, tagged with `owner`, for the memory sampler. Agents still
    /// starting (no process yet) or already stopped are left out.
    pub(super) fn agent_roots(
        &self,
        owner: &intent_services::RemoteOwner,
    ) -> Vec<intent_services::RemoteAgentRoot> {
        lock(&self.inner.state)
            .agents
            .values()
            .filter_map(|agent| {
                let root_pid = (*lock(&agent.root_pid))?;
                Some(intent_services::RemoteAgentRoot {
                    agent_id: agent.id.clone(),
                    workspace_id: agent.workspace_id.clone(),
                    provider: agent.provider.clone(),
                    model: agent.model.clone(),
                    agent_name: agent.labels.agent_name.as_deref().map(str::to_owned),
                    workspace_title: agent.labels.workspace_title.as_deref().map(str::to_owned),
                    owner: owner.clone(),
                    root_pid,
                })
            })
            .collect()
    }

    /// The link agents forward control-plane calls on; the newest session
    /// sets it when it adopts its lease.
    /// This machine's home, for skill discovery.
    pub(super) fn home(&self) -> Option<PathBuf> {
        self.inner.home.clone()
    }

    pub(super) fn head_link(&self) -> &HeadLink {
        &self.inner.head
    }

    /// The binding of the newest link session; see `session.rs`.
    pub(super) fn link_gate(&self) -> &tokio::sync::Mutex<Option<LeaseBinding>> {
        &self.inner.link_gate
    }

    /// Changes whenever a record is appended.
    pub(super) fn appended(&self) -> watch::Receiver<NodeCounter> {
        self.inner.appended.subscribe()
    }

    /// Run `f` on the journal on a blocking thread.
    pub(super) async fn with_journal<T, F>(&self, f: F) -> intent_core::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Journal) -> intent_core::Result<T> + Send + 'static,
    {
        let journal = self.inner.journal.clone();
        tokio::task::spawn_blocking(move || f(&mut lock(&journal)))
            .await
            .map_err(|e| intent_core::Error::Internal(format!("node-journal: {e}")))?
    }

    async fn append(&self, entry: JournalEntry) -> Option<JournalRecord> {
        match self
            .with_journal(move |journal| journal.append(entry))
            .await
        {
            Ok(record) => {
                let seq = record.seq;
                self.inner
                    .appended
                    .send_modify(|last| *last = (*last).max(seq));
                Some(record)
            }
            Err(error) => {
                // Head cannot be told without the journal; the agent's
                // later records keep their order, this one is lost.
                tracing::error!(%error, "node: journal append failed");
                None
            }
        }
    }

    /// Start `command`'s agent. A refusal (capacity, unknown provider) is
    /// journaled before this returns, so refusals keep the order of the
    /// frames that caused them; a failed start is journaled as the agent's
    /// exit with an error code.
    pub(super) async fn launch(&self, command: LaunchCommand) {
        // A launch brings an ended agent back (a restored agent keeps its
        // id): it may hold secrets again.
        lock(&self.inner.head_session)
            .ended
            .remove(&command.agent_id);
        self.claim_checkout(&command.agent_id).await;
        // Provider discovery reads the filesystem; keep it off the runtime.
        let launch = self.inner.launch.clone();
        let provider = command.provider.clone();
        let resolved = tokio::task::spawn_blocking(move || launch.resolve(&provider))
            .await
            .unwrap_or_else(|_| Err("provider resolution failed".to_owned()));
        if let Some(refusal) = self.begin_launch(command, resolved) {
            self.append(refusal).await;
        }
    }

    /// Admit and start a launch, or return the refusal to journal.
    fn begin_launch(
        &self,
        command: LaunchCommand,
        resolved: Result<ProviderLaunch, String>,
    ) -> Option<JournalEntry> {
        let LaunchCommand {
            agent_id,
            workspace_id,
            provider,
            model,
            base_commit,
            repo_key,
            profile_env,
            agent_features,
            is_sub_agent,
            checkout,
            mcp_proxies,
            compact_tool_descriptions,
            checkout_of,
            forge,
            forge_fetch,
            git_identity,
            new_machine,
            resumed_from_disk,
            harness_version,
            labels,
        } = command;
        let git_identity = git_identity.map(|identity| *identity);
        let forge = forge.map(|forge| *forge);
        let forge_fetch = forge_fetch.map(|fetch| *fetch);
        let prepared = checkout.is_some();
        let both = prepared && checkout_of.is_some();
        let setup = checkout
            .as_ref()
            .and_then(|checkout| checkout.setup.clone());
        let submodules = checkout
            .map(|checkout| checkout.submodules)
            .unwrap_or_default();
        let refused = |error: String, failure: Option<ExitFailure>| JournalEntry {
            owner: agent_id.clone().into(),
            workspace_id: workspace_id.clone(),
            turn_id: None,
            recorded_at: intent_core::now_iso(),
            body: JournalBody::Exited {
                exit_code: None,
                error: Some(error),
                failure,
            },
        };
        let exited = |error: String| refused(error, None);
        let mut state = lock(&self.inner.state);
        if state.closing {
            drop(state);
            return Some(exited(format!(
                "{}: the node is shutting down",
                codes::SHUTTING_DOWN
            )));
        }
        if both {
            drop(state);
            return Some(exited(format!(
                "{}: a launch into another agent's checkout carries no checkout of its own",
                codes::CHECKOUT
            )));
        }
        let checkout_of = match checkout_of {
            None => None,
            Some(CheckoutOf::Agent(owner)) => Some(owner),
            Some(CheckoutOf::Home(_)) => {
                drop(state);
                return Some(exited(format!(
                    "{}: this machine holds no such home",
                    codes::CHECKOUT
                )));
            }
        };
        if let Some(forge) = &forge {
            if checkout_of.is_some() || is_sub_agent || !forge.is_valid() {
                drop(state);
                return Some(exited(format!(
                    "{}: the launch's forge branch or remote is not usable",
                    codes::CHECKOUT
                )));
            }
        }
        if let Some(fetch) = &forge_fetch {
            let sub_agent = is_sub_agent || checkout_of.is_some();
            if forge.is_some() || !sub_agent || fetch.owner_repo().is_none() {
                drop(state);
                return Some(exited(format!(
                    "{}: the launch's forge fetch remote is not usable",
                    codes::CHECKOUT
                )));
            }
        }
        if state.agents.contains_key(&agent_id) {
            // The running agent's own records are what head should see; a
            // duplicate launch is not journaled as its exit.
            tracing::warn!(agent = %agent_id, "node: launch for an agent that is already running");
            return None;
        }
        let launch = match resolved {
            Ok(launch) => launch,
            Err(reason) => {
                drop(state);
                return Some(exited(format!(
                    "{}: provider {provider:?} is not available on this machine ({reason})",
                    codes::PROVIDER
                )));
            }
        };
        let profile = match profile_env
            .map(|wire| self.node_profile(&wire.0))
            .transpose()
        {
            Ok(profile) => profile,
            Err(error) => {
                drop(state);
                return Some(exited(format!("{}: {error}", codes::PROFILE)));
            }
        };
        // Last, so nothing above holds a slot it then gives back.
        if let Err(limit) = self.inner.admission.try_admit(&agent_id) {
            drop(state);
            return Some(refused(
                format!("{}: {limit}", codes::CAPACITY),
                Some(ExitFailure {
                    code: codes::CAPACITY.to_owned(),
                    data: Some(limit),
                }),
            ));
        }
        let (stop_tx, stop_rx) = oneshot::channel();
        let (ready_tx, ready_rx) = watch::channel(None);
        let (journaled_tx, journaled_rx) = watch::channel(ExitRecord::Pending);
        let (marks_tx, marks_rx) = mpsc::unbounded_channel();
        let (notes_tx, notes_rx) = mpsc::unbounded_channel();
        let agent = Arc::new(Agent {
            id: agent_id.clone(),
            workspace_id: workspace_id.clone(),
            provider: provider.clone(),
            model: model.clone(),
            labels: *labels,
            root_pid: Mutex::new(None),
            base_commit,
            repo_key,
            submodules,
            prepared,
            setup,
            setup_output: Mutex::new(None),
            gates: workspace_api::Gates {
                agent_features,
                is_sub_agent,
                // Head's call, as it decides whether the instructions carry
                // the condensed reference the compact description points at.
                compact_tool_descriptions,
                mcp_proxies,
                harness_version: harness_version.map(String::from),
                git_identity: git_identity.clone(),
            },
            worktree: Mutex::new(None),
            notice: Mutex::new(None),
            new_machine: new_machine.map(|machine| machine.0),
            resumed_from_disk,
            load_recorded: std::sync::atomic::AtomicBool::new(resumed_from_disk),
            skills_catalog: Mutex::new(None),
            wip: self.push_lock(checkout_of.as_ref().unwrap_or(&agent_id)),
            checkout_of: checkout_of.clone(),
            final_stop: std::sync::atomic::AtomicBool::new(false),
            final_push: Mutex::new(None),
            stop: Mutex::new(Some(stop_tx)),
            journaled: journaled_rx,
            ready: ready_rx,
            exit_detail: Mutex::new(None),
            busy: Mutex::new(false),
            descendants: Arc::new(Descendants::default()),
            run_marker: intent_core::node_link::LinkId::new_v4().to_string(),
            marks: marks_tx,
            activity: ActivityTracker::new(),
            redactor: Arc::new(Redactor::with_forge(
                profile.as_ref(),
                Some(self.inner.head.forge().clone()),
            )),
            forge_lease: Mutex::new(None),
            forge,
            forge_fetch,
            git_identity,
            npx_launch_dir: Mutex::new(None),
            default_account: profile.is_none().then(|| {
                (
                    intent_providers::base_provider_id(&provider).to_owned(),
                    self.inner.auth_failed.clone(),
                )
            }),
        });
        state.agents.insert(agent_id.clone(), agent.clone());
        state.exit_details.remove(&agent_id);
        match checkout_of {
            Some(owner) => state.shares.insert(agent_id.clone(), owner),
            None => state.shares.remove(&agent_id),
        };
        state.workspaces.insert(agent_id, workspace_id);
        drop(state);
        // caller-binding: allow — node-local journal pump; the node has no service layer
        let pump = tokio::spawn(self.clone().pump(
            (agent.id.clone(), agent.workspace_id.clone()),
            (agent.activity.clone(), agent.redactor.clone()),
            notes_rx,
            marks_rx,
            journaled_tx,
        ));
        {
            let mut pumps = lock(&self.inner.pumps);
            pumps.retain(|pump| !pump.is_finished());
            pumps.push(pump);
        }
        // caller-binding: allow — node-local provider process; the node has no service layer
        tokio::spawn(self.clone().run_agent(
            agent,
            (launch, profile),
            model,
            notes_tx,
            ready_tx,
            stop_rx,
        ));
        None
    }

    /// Journal the agent's notifications and marks in arrival order. A mark
    /// is taken only once every notification already queued is journaled,
    /// so a turn end never precedes that turn's output, and the pump alone
    /// decides which turn a notification belongs to: the turn it was
    /// received during, not whichever turn is current when it is written.
    /// The pump ends once the provider connection and every holder of the
    /// agent are gone, so a turn that ends after the process exited is
    /// still journaled.
    async fn pump(
        self,
        (agent_id, workspace_id): (AgentId, WorkspaceId),
        (activity, redactor): (ActivityTracker, Arc<Redactor>),
        mut notes: mpsc::UnboundedReceiver<IncomingNotification>,
        mut marks: mpsc::UnboundedReceiver<Mark>,
        journaled: watch::Sender<ExitRecord>,
    ) {
        let mut turn: Option<String> = None;
        let mut replaying = false;
        loop {
            let (turn_id, body) = tokio::select! {
                biased;
                Some(note) = notes.recv() => {
                    activity.touch();
                    if replaying || note.method != "session/update" {
                        continue;
                    }
                    let mut notification = note.params;
                    redactor.notification(&mut notification);
                    (turn.clone(), JournalBody::Notification { notification })
                }
                Some(mark) = marks.recv() => match mark {
                    Mark::Started { base_commit } => {
                        (None, JournalBody::Started { base_commit })
                    }
                    Mark::TurnStarted { turn_id, ack } => {
                        turn = Some(turn_id);
                        let _ = ack.send(());
                        continue;
                    }
                    Mark::Replaying { ack } => {
                        replaying = true;
                        let _ = ack.send(());
                        continue;
                    }
                    Mark::Replayed { ack } => {
                        settle_replay(&mut notes).await;
                        replaying = false;
                        if let Some(ack) = ack {
                            let _ = ack.send(());
                        }
                        continue;
                    }
                    Mark::TurnEnded { turn_id, stop_reason, error } => {
                        turn = None;
                        let error = error.map(|error| redactor.text(error));
                        (Some(turn_id), JournalBody::TurnEnded { stop_reason, error })
                    }
                    Mark::Exited { exit_code, error } => {
                        let error = error.map(|error| redactor.text(error));
                        (None, JournalBody::Exited {
                            exit_code,
                            error,
                            failure: None,
                        })
                    }
                },
                else => break,
            };
            let exit = matches!(body, JournalBody::Exited { .. });
            let appended = self
                .append(JournalEntry {
                    owner: agent_id.clone().into(),
                    workspace_id: workspace_id.clone(),
                    turn_id,
                    recorded_at: intent_core::now_iso(),
                    body,
                })
                .await
                .is_some();
            if exit {
                let state = if appended {
                    ExitRecord::Journaled
                } else {
                    ExitRecord::Lost
                };
                self.exit_settled(&agent_id, &journaled, state);
            }
        }
        // The pump ended without settling the exit: it never will be.
        if *journaled.borrow() == ExitRecord::Pending {
            self.exit_settled(&agent_id, &journaled, ExitRecord::Lost);
        }
    }

    /// The agent's exit record is journaled, or never will be: release its
    /// stop waits with `state` and forget it as exiting, so a later stop
    /// finds it not running.
    fn exit_settled(
        &self,
        agent_id: &AgentId,
        journaled: &watch::Sender<ExitRecord>,
        state: ExitRecord,
    ) {
        journaled.send_replace(state);
        let mut state = lock(&self.inner.state);
        if state
            .exiting
            .get(agent_id)
            .is_some_and(|waiting| waiting.same_channel(&journaled.subscribe()))
        {
            state.exiting.remove(agent_id);
        }
    }

    /// Spawn the provider, set up its ACP session, then wait for it to exit
    /// or be stopped. Always ends with exactly one exit mark and frees the
    /// agent's capacity slot.
    async fn run_agent(
        self,
        agent: Arc<Agent>,
        (launch, profile): (ProviderLaunch, Option<ProfileEnv>),
        model: Option<String>,
        notes: mpsc::UnboundedSender<IncomingNotification>,
        ready: watch::Sender<Option<Arc<Ready>>>,
        mut stop: oneshot::Receiver<()>,
    ) {
        let output = agent.prepared.then(|| {
            let output = SetupOutput::new(agent.id.clone(), self.inner.head.clone());
            *lock(&agent.setup_output) = Some(output.clone());
            output
        });
        let progress = |line: &str| {
            if let Some(output) = &output {
                output.line(line);
            }
        };
        let prepared = if let Some(owner) = &agent.checkout_of {
            Some(self.shared_checkout(&agent.id, owner))
        } else {
            tokio::select! {
                prepared = self.inner.checkouts.prepare(
                    &agent.id,
                    &agent.workspace_id,
                    (&agent.base_commit, &agent.repo_key),
                    &agent.submodules,
                    &progress,
                ) => Some(prepared),
                _ = &mut stop => None,
            }
        };
        // A top-level agent's own checkout gets its forge branch and
        // `origin`, or loses them on a launch without (nodeProtocol 15).
        // A sub-agent that fetches through the backend gets an `origin`
        // when its checkout has none (nodeProtocol 17).
        let prepared = match prepared {
            Some(Ok(ready)) => {
                let configured = async {
                    if agent.checkout_of.is_none() {
                        self.inner
                            .checkouts
                            .forge(&agent.id, &ready.worktree, agent.forge.as_ref())
                            .await?;
                    }
                    if let Some(fetch) = &agent.forge_fetch {
                        self.inner
                            .checkouts
                            .forge_origin(&ready.worktree, &fetch.remote_url)
                            .await?;
                    }
                    Ok::<(), anyhow::Error>(())
                };
                Some(configured.await.map(|()| ready))
            }
            other => other,
        };
        let forge_socket = self.forge_socket(&agent).unwrap_or_else(|error| {
            tracing::warn!(agent = %agent.id, %error, "node: no forge fetch path for the sub-agent");
            None
        });
        let started = match prepared {
            None => Err(None),
            Some(Err(error)) => Err(Some(format!("{}: {error:#}", codes::CHECKOUT))),
            Some(Ok(Prepared {
                worktree: cwd,
                tmp,
                unrestored,
                fresh,
            })) => {
                *lock(&agent.worktree) = Some(cwd.clone());
                let mut settled = match &output {
                    Some(output) => {
                        self.settle_setup(
                            &agent,
                            (&cwd, &tmp),
                            fresh,
                            agent.setup.as_ref(),
                            output,
                            &mut stop,
                        )
                        .await
                    }
                    None => Settled::default(),
                };
                if output.is_some() && !settled.stopped {
                    unsent_submodules(&mut settled, &cwd).await;
                }
                *lock(&agent.setup_output) = None;
                if settled.stopped {
                    Err(None)
                } else {
                    let forge_notice = self.forge_notice(&agent);
                    let notice = super::checkout::first_turn_notice(
                        agent.new_machine.as_deref(),
                        agent.resumed_from_disk,
                        &unrestored,
                        agent.setup.is_some(),
                    );
                    let notices = [notice, settled.notice, forge_notice]
                        .into_iter()
                        .flatten()
                        .reduce(|all, next| format!("{all}\n\n{next}"));
                    *lock(&agent.notice) = Some(first_turn_preamble(notices.as_deref()));
                    let mut helper_env = self.forge_helper_env(&agent);
                    if let Some(socket) = &forge_socket {
                        helper_env.extend(Self::forge_fetch_env(&agent, socket));
                    }
                    self.find_skills(&agent, &cwd).await;
                    match workspace_api::serve(
                        &agent.id,
                        &agent.workspace_id,
                        &cwd,
                        (&agent.gates, settled.overlay),
                        (self.inner.head.clone(), self.push_now_for(&agent.id)),
                        &self.inner.mcp_bridge_exe,
                    )
                    .await
                    {
                        Err(error) => Err(Some(format!(
                            "{}: the workspace API server did not start: {error}",
                            codes::START
                        ))),
                        Ok((bridge, mcp)) => Ok((bridge, mcp, cwd, tmp, helper_env)),
                    }
                }
            }
        };
        let (exit_code, error) = match started {
            Err(error) => (None, error),
            Ok((bridge, mcp, cwd, tmp, helper_env)) => {
                let mut retried = false;
                let result = loop {
                    let spawned = self.start(
                        &agent,
                        (&launch, profile.as_ref()),
                        model.as_deref(),
                        notes.clone(),
                        (cwd.clone(), &tmp, helper_env.clone()),
                    );
                    let (child, connection, serve, cwd) = match spawned {
                        Ok(spawned) => spawned,
                        Err(error) => break (None, Some(format!("{}: {error}", codes::START))),
                    };
                    let timeout = self.initialize_timeout(retried);
                    let outcome = self
                        .attempt(
                            &agent,
                            (&launch, timeout),
                            (&connection, child),
                            &mut stop,
                            &ready,
                            (&cwd, mcp.clone()),
                        )
                        .await;
                    serve.abort();
                    // One more start for a provider that did not come up in
                    // time or exited before it was ready: a machine that has
                    // just woken is often still slow to start one.
                    if !retried && outcome.start_may_recover() {
                        retried = true;
                        // Not the outcome itself: its error can echo a credential.
                        let timed_out = matches!(outcome, Outcome::StartFailed(..));
                        tracing::info!(agent = %agent.id, timed_out, "node: provider did not start; starting it once more");
                        // The first start's processes are gone.
                        agent
                            .descendants
                            .publish(agent.descendants.begin(), Vec::new());
                        continue;
                    }
                    break match outcome {
                        Outcome::Exited(code) | Outcome::ExitedAtStart(code, None) => (code, None),
                        Outcome::ExitedAtStart(code, Some(error)) => {
                            (code, Some(format!("{}: {error}", codes::START)))
                        }
                        Outcome::Stopped => (None, None),
                        Outcome::StartFailed(error, _) => {
                            (None, Some(format!("{}: {error}", codes::START)))
                        }
                        Outcome::SignedOut(provider) => (None, Some(signed_out_error(&provider))),
                    };
                };
                // The agent's workspace API stops with its provider.
                drop(bridge);
                result
            }
        };
        let detail = exit_detail(exit_code, error.as_deref());
        *lock(&agent.exit_detail) = Some(detail.clone());
        // Turns still waiting for the session see it is gone.
        drop(ready);
        // Whatever the agent did is sent back before head hears it exited.
        self.push_wip(&agent, Push::Exit).await;
        {
            let mut state = lock(&self.inner.state);
            if state
                .agents
                .get(&agent.id)
                .is_some_and(|current| Arc::ptr_eq(current, &agent))
            {
                state.agents.remove(&agent.id);
                state.exit_details.insert(agent.id.clone(), detail);
                self.inner.admission.release(&agent.id);
                // Its process is gone: its token no longer needs masking.
                *lock(&agent.forge_lease) = None;
                state
                    .exiting
                    .insert(agent.id.clone(), agent.journaled.clone());
            }
        }
        let _ = agent.marks.send(Mark::Exited { exit_code, error });
    }

    /// What a remote agent's first turn says about GitHub (nodeProtocol
    /// 15): a top-level agent with `forge` learns its branch and how to
    /// push; a sub-agent with the backend's token is told to read only.
    fn forge_notice(&self, agent: &Agent) -> Option<String> {
        if let Some(forge) = &agent.forge {
            let local = intent_core::node_link::forge_agent_branch(&agent.id);
            let branch = &forge.branch;
            return Some(format!(
                "Your checkout is on the local branch `{local}`, which tracks `{branch}` on `origin` ({}). A plain `git push` updates `{branch}` on GitHub, and `git pull` and `git rebase` follow it. Other agents of this workspace can push to `{branch}` too: when a push is rejected, pull or rebase, then push again. To open a pull request, run `gh pr create --head {branch}`.",
                forge.remote_url
            ));
        }
        let sub_agent = agent.gates.is_sub_agent || agent.checkout_of.is_some();
        let token = (sub_agent && self.inner.head.forge().get().is_some()).then(|| {
            "`GH_TOKEN` is set so that `gh` can read from GitHub. Use it only to read: do not push, and do not create or change anything on GitHub. Your work goes back to your parent agent through Intent.".to_owned()
        });
        let fetch = (agent.forge_fetch.is_some() && self.inner.forge_git.is_some()).then(|| {
            "`git fetch origin` gets the current branches from GitHub through Intent. `git push` to GitHub is refused: your work goes back to your parent agent through Intent.".to_owned()
        });
        [token, fetch]
            .into_iter()
            .flatten()
            .reduce(|all, next| format!("{all} {next}"))
    }

    /// A sub-agent's own forge helper socket (nodeProtocol 17), when it
    /// fetches GitHub through the backend.
    fn forge_socket(&self, agent: &Agent) -> anyhow::Result<Option<ForgeSocket>> {
        let (Some(_), Some(git), Some(main)) = (
            &agent.forge_fetch,
            &self.inner.forge_git,
            &self.inner.git_socket,
        ) else {
            return Ok(None);
        };
        // Named by this run too: a quick relaunch binds its own socket while
        // the previous run's is still being removed.
        let path = super::storage::forge_socket_path(
            main,
            &format!("{}/{}", agent.id.0, agent.run_marker),
            &super::storage::runtime_base(),
        )?;
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        let listener = tokio::net::UnixListener::bind(&path)?;
        let remote =
            intent_git::node_git::AgentRemote::new(agent.workspace_id.as_str(), &agent.id.0)
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .forge();
        // caller-binding: allow — machine-local git helper socket for one sub-agent
        let task = tokio::spawn(intent_transport::node_link::serve_forge_helpers(
            listener,
            git.sessions.clone(),
            remote,
        ));
        Ok(Some(ForgeSocket { path, task }))
    }

    /// The git environment of a sub-agent that fetches GitHub through the
    /// backend (nodeProtocol 17): its GitHub URLs rewritten to its forge
    /// remote, reached through its own socket.
    fn forge_fetch_env(agent: &Agent, socket: &ForgeSocket) -> Vec<(String, String)> {
        let Some(fetch) = &agent.forge_fetch else {
            return Vec::new();
        };
        let Ok(remote) =
            intent_git::node_git::AgentRemote::new(agent.workspace_id.as_str(), &agent.id.0)
        else {
            return Vec::new();
        };
        let inherited = std::env::var(intent_git::auth::GIT_CONFIG_PARAMETERS_ENV).ok();
        let mut env = intent_git::auth::forge_fetch_env(
            &remote.forge().url(),
            &fetch.remote_url,
            inherited.as_deref(),
        );
        env.push((
            intent_git::node_git::SOCKET_ENV.to_owned(),
            socket.path.to_string_lossy().into_owned(),
        ));
        env
    }

    /// The git credential helper environment of an agent with `forge`
    /// (nodeProtocol 15): none for any other agent, so a sub-agent sharing
    /// its checkout never gets it.
    fn forge_helper_env(&self, agent: &Agent) -> Vec<(String, String)> {
        let (Some(_), Some(socket)) = (&agent.forge, &self.inner.git_socket) else {
            return Vec::new();
        };
        let exe = self.inner.mcp_bridge_exe.to_string_lossy().into_owned();
        let inherited = std::env::var(intent_git::auth::GIT_CONFIG_PARAMETERS_ENV).ok();
        intent_git::auth::node_helper_env(&exe, socket, inherited.as_deref())
    }

    /// Run or account for the setup of a prepared checkout: a fresh one
    /// runs `command` (or records `skipped` without one) and journals
    /// `setupFinished`; a kept one journals only a setup a stop or restart
    /// interrupted (`failed`, no re-run).
    async fn settle_setup(
        &self,
        agent: &Agent,
        (cwd, tmp): (&Path, &Path),
        fresh: bool,
        command: Option<&intent_core::node_link::SetupCommand>,
        output: &Arc<SetupOutput>,
        stop: &mut oneshot::Receiver<()>,
    ) -> Settled {
        let status = match (fresh, command) {
            (false, _) => match setup::read_status(cwd) {
                Some(status) if status.state == setup::Recorded::Running => {
                    setup::exclude_interrupted(cwd, &agent.submodules).await;
                    status.interrupted()
                }
                Some(kept) => {
                    return Settled {
                        overlay: Some(kept.overlay()),
                        ..Settled::default()
                    }
                }
                // Kept from a launch that never got as far as setup (or
                // from a machine without it): record a result once, so the
                // backend does not wait for one.
                None if command.is_some() => SetupStatus::never_ran(),
                None => SetupStatus::skipped(),
            },
            (true, None) => SetupStatus::skipped(),
            (true, Some(command)) => {
                match self
                    .run_setup(agent, (cwd, tmp), command, output, stop)
                    .await
                {
                    setup::Ran::Finished(status) => status,
                    setup::Ran::Stopped => {
                        return Settled {
                            stopped: true,
                            ..Settled::default()
                        }
                    }
                }
            }
        };
        setup::write_status(cwd, &status);
        if let Some(state) = status.finished_state() {
            self.append(agent_entry(
                agent,
                JournalBody::SetupFinished {
                    state,
                    exit_code: status.exit_code,
                    error: status.error.clone(),
                    finished_at: status
                        .finished_at
                        .clone()
                        .unwrap_or_else(intent_core::now_iso),
                },
            ))
            .await;
        }
        let (tail, _) = output.tail();
        Settled {
            notice: setup::failed_notice(&status, cwd, &tail),
            overlay: Some(status.overlay()),
            stopped: false,
        }
    }

    /// Journal `setupStarted`, then run the script. Until it finishes the
    /// checkout records it running, so a stop or restart reads as
    /// interrupted at the next launch.
    async fn run_setup(
        &self,
        agent: &Agent,
        (cwd, tmp): (&Path, &Path),
        command: &intent_core::node_link::SetupCommand,
        output: &Arc<SetupOutput>,
        stop: &mut oneshot::Receiver<()>,
    ) -> setup::Ran {
        let main_checkout = match self.inner.checkouts.main_checkout(&agent.repo_key) {
            Ok(dir) => dir,
            Err(error) => {
                return setup::Ran::Finished(SetupStatus {
                    state: setup::Recorded::Failed,
                    exit_code: None,
                    error: Some(format!(
                        "{}: could not create MAIN_CHECKOUT: {error:#}",
                        setup::codes::FAILED
                    )),
                    started_at: None,
                    finished_at: Some(intent_core::now_iso()),
                });
            }
        };
        let started_at = intent_core::now_iso();
        setup::write_status(
            cwd,
            &SetupStatus {
                state: setup::Recorded::Running,
                exit_code: None,
                error: None,
                started_at: Some(started_at.clone()),
                finished_at: None,
            },
        );
        self.append(agent_entry(
            agent,
            JournalBody::SetupStarted {
                started_at: started_at.clone(),
            },
        ))
        .await;
        setup::run(
            &self.inner.pty,
            setup::SetupRun {
                command,
                worktree: cwd,
                tmp,
                main_checkout: &main_checkout,
                pins: &agent.submodules,
            },
            output,
            started_at,
            stop,
        )
        .await
    }

    /// A new link to head is up: resend the kept output of every setup
    /// still under way, from `seq` 0.
    pub(super) fn resend_setup_output(&self, link: &intent_transport::node_link::NodeLink) {
        let outputs: Vec<Arc<SetupOutput>> = lock(&self.inner.state)
            .agents
            .values()
            .filter_map(|agent| lock(&agent.setup_output).clone())
            .collect();
        for output in outputs {
            output.resend(link);
        }
    }

    fn start(
        &self,
        agent: &Agent,
        (launch, profile): (&ProviderLaunch, Option<&ProfileEnv>),
        model: Option<&str>,
        notes: mpsc::UnboundedSender<IncomingNotification>,
        (cwd, tmp, helper_env): (PathBuf, &Path, Vec<(String, String)>),
    ) -> anyhow::Result<(Child, Arc<Connection>, tokio::task::JoinHandle<()>, PathBuf)> {
        let (requests_tx, mut requests) = mpsc::unbounded_channel::<IncomingRequest>();
        let hooks = ConnectionHooks {
            requests: Some(requests_tx),
            notifications: Some(notes),
            agent_id: Some(agent.id.0.clone()),
            ..ConnectionHooks::default()
        };
        let mut options = SpawnOptions::new(&launch.config);
        options.cwd = Some(&cwd);
        options.provider_binary = launch.binary.as_deref();
        options.npx_fallback_binary = launch.npx_fallback_binary.as_deref();
        options.npx_fallback_package = launch.npx_fallback_package;
        options.model = model;
        options.profile_env = profile;
        if agent.forge_fetch.is_some() {
            options.path_prefix = self
                .inner
                .forge_git
                .as_ref()
                .map(|git| git.helper_dir.as_path());
        }
        options.extra_env.clone_from(&launch.extra_env);
        options.extra_env.extend(remote_launch_env(&launch.config));
        options
            .extra_env
            .insert(RUN_MARKER_ENV.to_owned(), agent.run_marker.clone());
        // The agent's private temp folder lives with its checkout, so it
        // survives relaunches, and beats inherited and profile values like a
        // local launch's.
        for key in intent_core::AGENT_TMP_ENV_KEYS {
            options
                .extra_env
                .insert(key.to_owned(), tmp.to_string_lossy().into_owned());
        }
        // The backend's commit identity, as Intent wiring over inherited
        // values (a profile cannot set these keys); only in this process's
        // environment, never in git config.
        if let Some((name, email)) = agent.git_identity.as_ref().and_then(GitIdentity::usable) {
            for key in intent_git::identity::GIT_IDENTITY_ENV_VARS {
                let value = if key.ends_with("EMAIL") { email } else { name };
                options.extra_env.insert(key.to_owned(), value.to_owned());
            }
        }
        // The backend's GitHub token (nodeProtocol 15) for every agent:
        // Intent wiring, so it beats a profile's value. The lease keeps it
        // masked in every journal while this process may hold it.
        let spawned_with = self.inner.head.forge().spawn();
        if let Some((token, _)) = &spawned_with {
            options
                .extra_env
                .insert(GH_TOKEN_ENV.to_owned(), token.as_str().to_owned());
        }
        *lock(&agent.forge_lease) = spawned_with.map(|(_, lease)| lease);
        // A top-level agent's git asks the machine for the live token
        // (nodeProtocol 15); only its own process gets the helper.
        options.extra_env.extend(helper_env);
        let spawned = intent_acp::spawn_provider(&options, hooks)?;
        let (child, connection, npx_launch_dir, agent_tmp_dir) = spawned.into_parts();
        // An npx launch runs from its launch directory; keep it until the
        // agent is gone.
        *lock(&agent.npx_launch_dir) = npx_launch_dir;
        // Remote agents set no `agent_tmp_root`: their folder is the
        // checkout's, so no launch folder was created.
        debug_assert!(agent_tmp_dir.is_none());
        drop(agent_tmp_dir);
        let connection = Arc::new(connection);
        let handler = ClientRequestHandler::new(
            agent.workspace_id.clone(),
            agent.id.clone(),
            agent.id.0.clone(),
            FileService::new(&cwd),
            Arc::new(PermissionRegistry::new()),
            self.inner.launch.policy(),
            Arc::new(NodeSink),
        )
        .with_auto_allow_once(launch.config.auto_allow_once)
        .with_allow_always_scopes(launch.config.allow_always_scopes);
        let serving = connection.clone();
        // caller-binding: allow — serves the provider's file and permission callbacks locally; the node has no service layer
        let serve = tokio::spawn(async move {
            while let Some(request) = requests.recv().await {
                if handler.serve(&serving, request).await.is_err() {
                    break;
                }
            }
        });
        Ok((child, connection, serve, cwd))
    }

    /// Supervise one start of the provider, then clean up after it: its
    /// process tree, and whatever it left behind.
    async fn attempt(
        &self,
        agent: &Agent,
        launch: (&ProviderLaunch, Duration),
        (connection, mut child): (&Arc<Connection>, Child),
        stop: &mut oneshot::Receiver<()>,
        ready: &watch::Sender<Option<Arc<Ready>>>,
        (cwd, mcp): (&std::path::Path, Vec<intent_acp::session::McpServer>),
    ) -> Outcome {
        // Read before waiting: a reaped child no longer reports it, and its
        // process group can outlive it.
        let pid = child.id();
        *lock(&agent.root_pid) = pid;
        let tracker = pid.map(|pid| {
            let descendants = agent.descendants.clone();
            // caller-binding: allow — node-local process snapshot; the node has no service layer
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(TRACK_INTERVAL);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    let scan = descendants.begin();
                    let found = intent_acp::descendant_pids(pid).await;
                    descendants.publish(scan, found);
                }
            })
        });
        let outcome = self
            .supervise(
                agent,
                launch,
                connection,
                &mut child,
                stop,
                ready,
                (cwd, mcp),
                (pid, &agent.descendants),
            )
            .await;
        if let Some(tracker) = tracker {
            tracker.abort();
        }
        // Whatever the provider left behind goes too, also when it exited by
        // itself: its process group, the helpers last seen as its
        // descendants (and theirs), and on Linux every process still
        // carrying this run's marker.
        let mut known = agent.descendants.pids();
        known.extend(marked_processes(&agent.run_marker).await);
        kill_tree(child, pid, &known).await;
        *lock(&agent.root_pid) = None;
        outcome
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "private step of run_agent over its locals"
    )]
    async fn supervise(
        &self,
        agent: &Agent,
        (launch, initialize_timeout): (&ProviderLaunch, Duration),
        connection: &Arc<Connection>,
        child: &mut Child,
        stop: &mut oneshot::Receiver<()>,
        ready: &watch::Sender<Option<Arc<Ready>>>,
        (cwd, mcp): (&std::path::Path, Vec<intent_acp::session::McpServer>),
        (pid, descendants): (Option<u32>, &Descendants),
    ) -> Outcome {
        // The agent's workspace MCP server and its proxied user servers ride
        // `session/new`, which every ACP agent accepts stdio servers on.
        // After a disk resume the provider's transcript is still on the
        // disk: load the session the agent last ran with, so the
        // conversation carries over (intent-hq/intent#9236).
        let recorded = agent
            .load_recorded
            .swap(false, std::sync::atomic::Ordering::SeqCst)
            .then(|| self.inner.checkouts.provider_session(&agent.id))
            .flatten()
            .filter(|recorded| {
                recorded.provider == agent.provider && recorded.model == agent.model
            });
        let setup = async {
            let handshake =
                intent_acp::handshake_within(connection, &launch.config, initialize_timeout)
                    .await?;
            if let Some(recorded) = recorded
                .filter(|_| intent_acp::session::supports_load_session(&handshake.initialize))
            {
                match load_recorded(agent, connection, cwd, mcp.clone(), &recorded.session_id).await
                {
                    Ok(()) => return Ok(recorded.session_id),
                    Err(error) => {
                        tracing::warn!(agent = %agent.id, %error, "node: could not load the agent's provider session; starting a new one");
                    }
                }
            }
            let session = intent_acp::session::new_session(connection, cwd, mcp, None).await?;
            Ok::<_, intent_acp::AcpError>(session.session_id.to_string())
        };
        let session = tokio::select! {
            result = setup => match result {
                Ok(session) => session,
                Err(error) => {
                    agent.note_auth_failure(&error);
                    if intent_services::provider_auth::is_auth_required(&error) {
                        // Only a Default can run on the machine's own sign-in
                        // instead; other providers keep the plain start code.
                        if let Some((provider, _)) = agent
                            .default_account
                            .as_ref()
                            .filter(|(p, _)| signs_in_on_the_machine(p))
                        {
                            return Outcome::SignedOut(provider.clone());
                        }
                        return Outcome::StartFailed(error.to_string(), false);
                    }
                    // A provider that exits at start often closes its output
                    // before its exit is seen: it exited, not failed setup.
                    if let Ok(status) = tokio::time::timeout(EXIT_SETTLE, child.wait()).await {
                        let code = status.ok().and_then(|s| s.code());
                        return Outcome::ExitedAtStart(code, Some(error.to_string()));
                    }
                    let timed_out = matches!(&error, intent_acp::AcpError::Timeout(method) if method == "initialize");
                    return Outcome::StartFailed(error.to_string(), timed_out);
                }
            },
            status = child.wait() => {
                return Outcome::ExitedAtStart(status.ok().and_then(|s| s.code()), None);
            }
            _ = &mut *stop => return Outcome::Stopped,
        };
        // Record what the provider started during setup before any prompt
        // can make it exit: once it has exited, helpers in their own process
        // groups are no longer its descendants.
        if let Some(pid) = pid {
            let scan = descendants.begin();
            let found = intent_acp::descendant_pids(pid).await;
            descendants.publish(scan, found);
        }
        let _ = agent.marks.send(Mark::Started {
            base_commit: agent.base_commit.clone(),
        });
        let recorded = ProviderSession {
            session_id: session.clone(),
            provider: agent.provider.clone(),
            model: agent.model.clone(),
        };
        if let Err(error) = self
            .inner
            .checkouts
            .record_provider_session(&agent.id, &recorded)
        {
            tracing::warn!(agent = %agent.id, error = %format!("{error:#}"), "node: could not record the agent's provider session");
        }
        ready.send_replace(Some(Arc::new(Ready {
            connection: connection.clone(),
            session_id: session,
        })));
        tokio::select! {
            status = child.wait() => Outcome::Exited(status.ok().and_then(|s| s.code())),
            _ = &mut *stop => Outcome::Stopped,
        }
    }

    /// The overlay a launch's profile runs with on this node: parsed
    /// strictly, then each account directory's leading `~` expanded against
    /// the node's home and required to exist. The error never renders a
    /// value.
    fn node_profile(&self, wire: &Value) -> Result<ProfileEnv, String> {
        let profile = ProfileEnv::from_node_wire(wire).ok_or_else(|| {
            "the selected provider profile's environment could not be read; no other account was used"
                .to_owned()
        })?;
        profile
            .for_node(self.inner.home.as_deref())
            .map_err(|missing| missing.to_string())
    }

    /// Run a turn. Its output streams as notifications; its end (or why it
    /// could not run) is journaled as a turn end, a refusal before this
    /// returns.
    pub(super) async fn prompt(&self, agent_id: &AgentId, turn_id: String, prompt: Vec<Value>) {
        if let Some(refusal) = self.begin_prompt(agent_id, turn_id, prompt) {
            self.append(refusal).await;
        }
    }

    /// Start a turn, or return the refusal to journal.
    fn begin_prompt(
        &self,
        agent_id: &AgentId,
        turn_id: String,
        prompt: Vec<Value>,
    ) -> Option<JournalEntry> {
        let (agent, workspace) = {
            let state = lock(&self.inner.state);
            (
                state.agents.get(agent_id).cloned(),
                state.workspaces.get(agent_id).cloned(),
            )
        };
        let refuse = |workspace_id: WorkspaceId, code: &str, detail: &str| JournalEntry {
            owner: agent_id.clone().into(),
            workspace_id,
            turn_id: Some(turn_id.clone()),
            recorded_at: intent_core::now_iso(),
            body: JournalBody::TurnEnded {
                stop_reason: "refusal".into(),
                error: Some(format!("{code}: {detail}")),
            },
        };
        let Some(agent) = agent else {
            if workspace.is_none() {
                tracing::warn!(agent = %agent_id, "node: prompt for an unknown agent");
            }
            let detail = lock(&self.inner.state)
                .exit_details
                .get(agent_id)
                .cloned()
                .unwrap_or_else(|| exit_detail(None, None));
            return workspace.map(|workspace| refuse(workspace, codes::NOT_RUNNING, &detail));
        };
        {
            let mut busy = lock(&agent.busy);
            if *busy {
                drop(busy);
                return Some(refuse(
                    agent.workspace_id.clone(),
                    codes::BUSY,
                    "a turn is already running",
                ));
            }
            *busy = true;
        }
        let runner = self.clone();
        // caller-binding: allow — node-local ACP turn; the node has no service layer
        tokio::spawn(async move {
            let (stop_reason, error) = run_turn(&agent, &turn_id, prompt).await;
            let _ = agent.marks.send(Mark::TurnEnded {
                turn_id,
                stop_reason,
                error,
            });
            *lock(&agent.busy) = false;
            // The agent is idle: send its work back.
            runner.push_wip(&agent, Push::TurnEnd).await;
        });
        None
    }

    /// Discover the agent's skills on this machine for its first prompt and,
    /// when the backend's last link negotiated protocol 12, journal them
    /// before it starts.
    async fn find_skills(&self, agent: &Agent, cwd: &std::path::Path) {
        let found = skills::discover(Some(cwd.to_path_buf()), self.inner.home.clone()).await;
        *lock(&agent.skills_catalog) = skills::catalog_block(&found);
        let reports = self.inner.head.last_protocol() >= SKILLS_PROTOCOL;
        if reports {
            self.append(agent_entry(agent, JournalBody::Skills { skills: found }))
                .await;
        }
    }

    /// Push the agent's work to head, if its checkout was prepared. Failures
    /// are logged: the next push sends the same refs again.
    async fn push_wip(&self, agent: &Agent, kind: Push) {
        let Some(worktree) = lock(&agent.worktree).clone() else {
            return;
        };
        // Once the daemon is stopping, only the agent's exit push runs: a
        // turn-end push, queued or under way, gives way to it, and the exit
        // push sends everything it would have within the shutdown budget.
        let mut shutting_down = self.inner.closing_signal.subscribe();
        let _serial = match kind {
            Push::Exit => agent.wip.lock().await,
            Push::TurnEnd => tokio::select! {
                serial = agent.wip.lock() => serial,
                Ok(_) = shutting_down.wait_for(|closing| *closing) => return,
            },
        };
        let final_stop = agent.final_stop.load(std::sync::atomic::Ordering::SeqCst);
        let closing = lock(&self.inner.state).closing;
        if closing && kind == Push::TurnEnd {
            return;
        }
        let owner = agent.checkout_owner();
        let push = |timeout| {
            self.inner
                .checkouts
                .push_wip(owner, &agent.workspace_id, &worktree, timeout)
        };
        let pushed = if kind == Push::Exit && (final_stop || closing) {
            // The agent's last push here, bounded like a final stop's.
            push(FINAL_PUSH_TIMEOUT).await
        } else {
            // Any other push gives way to shutdown: an ordinary one (up to
            // `WIP_PUSH_TIMEOUT`), or a turn's end during a final stop's
            // script stop, would hold the lock past the shutdown budget.
            // Head's ref update is atomic: its refs are left old or whole.
            let timeout = if final_stop {
                FINAL_PUSH_TIMEOUT
            } else {
                WIP_PUSH_TIMEOUT
            };
            let raced = tokio::select! {
                pushed = push(timeout) => Some(pushed),
                Ok(_) = shutting_down.wait_for(|closing| *closing) => None,
            };
            match (raced, kind) {
                (Some(pushed), _) => pushed,
                // An exit push is the last one: send it again, bounded.
                (None, Push::Exit) => push(FINAL_PUSH_TIMEOUT).await,
                (None, Push::TurnEnd) => return,
            }
        };
        if let Err(error) = &pushed {
            tracing::warn!(agent = %agent.id, error = %format!("{error:#}"), "node: WIP push failed");
        }
        if final_stop {
            *lock(&agent.final_push) = Some(if pushed.is_ok() {
                FinalPush::Pushed
            } else {
                FinalPush::Failed
            });
        }
    }

    /// End `agent_id` for good (a final stop, nodeProtocol 9): end its
    /// script runs, then stop it as [`Self::stop`] does, its exit push
    /// bounded by [`FINAL_PUSH_TIMEOUT`] (an agent that is not running is
    /// pushed then, when this machine knows its workspace), then remove its
    /// checkout. Resolves with the acknowledgement's outcome and push report,
    /// or `None` when the stop may not be acknowledged (its exit record was
    /// not journaled, or the checkout could not be removed): the backend
    /// resends it.
    ///
    /// The cleanup runs once per agent at a time: a repeat delivered while
    /// it runs (the backend resends until acknowledged) joins it and gets
    /// the same answer, and it runs to the end even if no caller waits.
    pub(super) fn final_stop(
        &self,
        agent_id: &AgentId,
    ) -> BoxFuture<'static, Option<(StopOutcome, FinalPush)>> {
        let mut done = {
            let mut state = lock(&self.inner.state);
            if let Some(running) = state.finalizing.get(agent_id) {
                running.clone()
            } else {
                let (finished, done) = watch::channel(None);
                state.finalizing.insert(agent_id.clone(), done.clone());
                let agent = state.agents.get(agent_id).cloned();
                drop(state);
                if let Some(agent) = &agent {
                    agent
                        .final_stop
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                }
                let runner = self.clone();
                let agent_id = agent_id.clone();
                // caller-binding: allow — node-local final-stop cleanup that every repeat joins
                tokio::spawn(async move {
                    // Ends the cleanup however the task ends (a panic or a
                    // cancel included): no entry is left for launches to
                    // wait on or later stops to join.
                    let mut guard = FinalizingGuard {
                        runner: runner.clone(),
                        agent_id: agent_id.clone(),
                        finished,
                        result: None,
                    };
                    // The agent ends for good: its secrets go first, and a
                    // put still on its way is refused.
                    runner.end_secrets(&agent_id);
                    // The agent's script runs end first, so nothing still
                    // writes to its checkout while it is pushed and removed.
                    if !runner.inner.scripts.stop_agent(&agent_id).await {
                        // A script that is still not reaped may still write
                        // to the checkout: stop the provider, but neither
                        // push nor remove, and do not acknowledge, so the
                        // backend resends the stop.
                        tracing::warn!(agent = %agent_id, "node: final stop not acknowledged: a script of the agent was not reaped");
                        drop(runner.stop(&agent_id));
                        return;
                    }
                    // The agents working in this checkout stop before it
                    // is pushed and removed (nodeProtocol 13).
                    if !runner.stop_sharers(&agent_id).await {
                        tracing::warn!(agent = %agent_id, "node: final stop not acknowledged: an agent sharing the checkout did not stop");
                        drop(runner.stop(&agent_id));
                        return;
                    }
                    let wait = runner.stop(&agent_id);
                    guard.result = runner.finish_final_stop(agent, &agent_id, wait).await;
                });
                done
            }
        };
        Box::pin(async move {
            match done.wait_for(Option::is_some).await {
                Ok(result) => (*result).flatten(),
                Err(_) => None,
            }
        })
    }

    async fn finish_final_stop(
        &self,
        agent: Option<Arc<Agent>>,
        agent_id: &AgentId,
        wait: StopWait,
    ) -> Option<(StopOutcome, FinalPush)> {
        let outcome = wait.wait().await?;
        let exit_push = agent.and_then(|agent| *lock(&agent.final_push));
        let push = match exit_push {
            Some(push) => push,
            None => self.push_final(agent_id).await,
        };
        // No requested push (`pushWip`) reads the checkout while it goes.
        let serial = self.push_lock(agent_id);
        let _serial = serial.lock().await;
        if let Err(error) = self.inner.checkouts.remove(agent_id).await {
            tracing::warn!(agent = %agent_id, error = %format!("{error:#}"), "node: could not remove a stopped agent's checkout; not acknowledging");
            return None;
        }
        Some((outcome, push))
    }

    /// The final push of an agent that is not running: `skipped` without a
    /// checkout here, or when this machine does not know its workspace
    /// (it already pushed when it exited).
    async fn push_final(&self, agent_id: &AgentId) -> FinalPush {
        // An agent sharing a checkout pushes it to its owner's refs.
        let owner = lock(&self.inner.state).checkout_owner(agent_id);
        let Some(worktree) = self.inner.checkouts.existing(&owner) else {
            return FinalPush::Skipped;
        };
        let Some(workspace_id) = lock(&self.inner.state).workspaces.get(&owner).cloned() else {
            return FinalPush::Skipped;
        };
        let serial = self.push_lock(&owner);
        let _serial = serial.lock().await;
        match self
            .inner
            .checkouts
            .push_wip(&owner, &workspace_id, &worktree, FINAL_PUSH_TIMEOUT)
            .await
        {
            Ok(()) => FinalPush::Pushed,
            Err(error) => {
                tracing::warn!(agent = %agent_id, error = %format!("{error:#}"), "node: final WIP push failed");
                FinalPush::Failed
            }
        }
    }

    /// The one push lock of `owner`'s checkout.
    fn push_lock(&self, owner: &AgentId) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = lock(&self.inner.push_locks);
        locks.retain(|_, held| held.strong_count() > 0);
        if let Some(held) = locks.get(owner).and_then(std::sync::Weak::upgrade) {
            return held;
        }
        let held = Arc::new(tokio::sync::Mutex::new(()));
        locks.insert(owner.clone(), Arc::downgrade(&held));
        held
    }

    /// The checkout of `owner` a launch with `checkoutOf` works in
    /// (nodeProtocol 13); nothing is fetched or set up.
    fn shared_checkout(&self, agent_id: &AgentId, owner: &AgentId) -> anyhow::Result<Prepared> {
        let removing = {
            let state = lock(&self.inner.state);
            state.sweeping.contains(owner) || state.finalizing.contains_key(owner)
        };
        match self.inner.checkouts.existing(owner) {
            Some(worktree) if !removing => Ok(Prepared {
                worktree,
                tmp: self.inner.checkouts.agent_tmp(agent_id)?,
                unrestored: Vec::new(),
                fresh: false,
            }),
            _ => Err(anyhow::anyhow!(
                "the checkout of agent {owner} is not on this machine"
            )),
        }
    }

    /// Stop every agent working in `owner`'s checkout, their script runs
    /// first, and wait until each one's exit is journaled. `false` when one
    /// could not be confirmed stopped.
    async fn stop_sharers(&self, owner: &AgentId) -> bool {
        let sharers = lock(&self.inner.state).live_sharers(owner);
        let mut stopped = true;
        for sharer in sharers {
            if !self.inner.scripts.stop_agent(&sharer).await {
                stopped = false;
            }
            stopped &= self.stop(&sharer).wait().await.is_some();
        }
        stopped
    }

    /// [`Self::push_now`] for `agent_id`'s `ws.git.commit` and
    /// `ws.agent.reportToParent`: `true` once head has the checkout. The
    /// lock wait and push together fit in the caller's budget. It holds the
    /// runner weakly, so the agent's MCP server does not keep it.
    fn push_now_for(&self, agent_id: &AgentId) -> workspace_api::PushNow {
        let runner = Arc::downgrade(&self.inner);
        let agent_id = agent_id.clone();
        Arc::new(move |budget| {
            let runner = runner.upgrade().map(|inner| Self { inner });
            let agent_id = agent_id.clone();
            Box::pin(async move {
                let Some(runner) = runner else { return false };
                // Like a turn-end push, it gives way to shutdown, so the
                // agent's exit push starts within the budget.
                let mut closing = runner.inner.closing_signal.subscribe();
                let push = async {
                    tokio::select! {
                        (pushed, _) = runner.push_now(&agent_id) => pushed == FinalPush::Pushed,
                        Ok(_) = closing.wait_for(|closing| *closing) => false,
                    }
                };
                tokio::time::timeout(budget, push)
                    .await
                    .unwrap_or_else(|_| {
                        tracing::warn!(agent = %agent_id, "node: in-call push ran out of time");
                        false
                    })
            })
        })
    }

    /// Push `agent_id`'s checkout now (`pushWip`, nodeProtocol 13), to its
    /// owner's refs, one push of that checkout at a time: `skipped` when
    /// it has no checkout here or the machine does not know its workspace.
    pub(super) async fn push_now(&self, agent_id: &AgentId) -> (FinalPush, Option<String>) {
        let (owner, workspace_id) = {
            let state = lock(&self.inner.state);
            let owner = state.checkout_owner(agent_id);
            let workspace_id = state.workspaces.get(&owner).cloned();
            (owner, workspace_id)
        };
        let (Some(worktree), Some(workspace_id)) =
            (self.inner.checkouts.existing(&owner), workspace_id)
        else {
            return (FinalPush::Skipped, None);
        };
        let serial = self.push_lock(&owner);
        let _serial = serial.lock().await;
        // A final stop or the sweep is removing the checkout: its final
        // push, not this one, is the last word.
        let removing = {
            let state = lock(&self.inner.state);
            state.finalizing.contains_key(&owner) || state.sweeping.contains(&owner)
        };
        if removing || self.inner.checkouts.existing(&owner).is_none() {
            return (FinalPush::Skipped, None);
        }
        match self
            .inner
            .checkouts
            .push_wip(&owner, &workspace_id, &worktree, FINAL_PUSH_TIMEOUT)
            .await
        {
            Ok(()) => (FinalPush::Pushed, None),
            Err(error) => {
                let error = format!("{error:#}");
                tracing::warn!(agent = %owner, %error, "node: requested WIP push failed");
                (FinalPush::Failed, Some(error))
            }
        }
    }

    /// Answer a `checkoutRead` (nodeProtocol 16): one piece of a snapshot
    /// of a file or folder in `read.agent_id`'s checkout, confined to it.
    /// Only an agent on the binding's lease whose checkout exists serves.
    pub(super) async fn checkout_read(&self, read: CheckoutRead) -> CheckoutReadReply {
        let worktree = {
            let head = lock(&self.inner.head_session);
            let state = lock(&self.inner.state);
            let on_lease = !head.ended.contains(&read.agent_id)
                && (head.lease_agents.contains(&read.agent_id)
                    || state.workspaces.contains_key(&read.agent_id));
            let owner = state.checkout_owner(&read.agent_id);
            drop((head, state));
            on_lease
                .then(|| self.inner.checkouts.existing(&owner))
                .flatten()
        };
        let answer = match worktree {
            None => Err(format!(
                "file-pull-source-unavailable: agent {} has no checkout on this machine",
                read.agent_id
            )),
            Some(_) if !(1..=MAX_LINK_ATTACHMENT_CHUNK_BYTES).contains(&read.length) => {
                Err("file-pull-mismatch: length must be 1 to 4194304".to_owned())
            }
            Some(worktree) => {
                let pulls = self.inner.pulls.clone();
                let source = read.agent_id.to_string();
                let request = intent_services::checkout_transfer::PieceRequest {
                    path: read.path.clone(),
                    offset: read.offset,
                    length: read.length,
                    transfer_id: read.transfer_id.clone(),
                };
                tokio::task::spawn_blocking(move || pulls.read(&source, &worktree, &request))
                    .await
                    .map_err(|error| format!("file-pull-source-unavailable: {error}"))
                    .and_then(|read| {
                        read.map_err(|error| match error {
                            intent_core::Error::InvalidParams(message) => message,
                            // Fixed text: an I/O error may name a path.
                            _ => "file-pull-source-unavailable: the machine could not read the checkout".to_owned(),
                        })
                    })
            }
        };
        let (outcome, piece, error) = match answer {
            Ok(piece) => (CheckoutReadOutcome::Ok, Some(piece), None),
            Err(error) => (CheckoutReadOutcome::Failed, None, Some(error)),
        };
        CheckoutReadReply {
            request_id: read.request_id,
            agent_id: read.agent_id,
            outcome,
            piece,
            error,
        }
    }

    /// Mark `agent_id` launched, so the bind sweep leaves its checkout, and
    /// wait out a removal of that checkout already under way (by the sweep
    /// or a final stop).
    async fn claim_checkout(&self, agent_id: &AgentId) {
        loop {
            let swept = self.inner.swept.notified();
            tokio::pin!(swept);
            swept.as_mut().enable();
            {
                let mut state = lock(&self.inner.state);
                state.launched.insert(agent_id.clone());
                if !state.sweeping.contains(agent_id) && !state.finalizing.contains_key(agent_id) {
                    return;
                }
            }
            swept.await;
        }
    }

    /// At a nodeProtocol 9 bind, before any frame of the new link is
    /// handled: the checkouts of this backend's agents that are neither in
    /// `lease_agents` nor running, exiting or being removed by a final stop.
    /// Launches from here on are marked, and the sweep leaves them.
    pub(super) fn sweep_candidates(&self, lease_agents: &[AgentId]) -> Vec<AgentId> {
        let on_disk = self.inner.checkouts.agents();
        let mut state = lock(&self.inner.state);
        state.launched.clear();
        // A checkout stays while an agent sharing it runs or is on the
        // lease (nodeProtocol 13).
        let in_use: HashSet<AgentId> = state
            .shares
            .iter()
            .filter(|(agent_id, _)| {
                lease_agents.contains(agent_id)
                    || state.agents.contains_key(*agent_id)
                    || state.exiting.contains_key(*agent_id)
            })
            .map(|(_, owner)| owner.clone())
            .collect();
        on_disk
            .into_iter()
            .filter(|agent_id| {
                !lease_agents.contains(agent_id)
                    && !in_use.contains(agent_id)
                    && !state.agents.contains_key(agent_id)
                    && !state.exiting.contains_key(agent_id)
                    && !state.finalizing.contains_key(agent_id)
            })
            .collect()
    }

    /// Remove the checkouts of `candidates` (from [`Self::sweep_candidates`])
    /// except those launched, exiting or finalizing since. Returns the agents
    /// swept.
    pub(super) async fn sweep_checkouts(&self, candidates: Vec<AgentId>) -> Vec<AgentId> {
        let mut swept = Vec::new();
        for agent_id in candidates {
            {
                let mut state = lock(&self.inner.state);
                if state.launched.contains(&agent_id)
                    || state.agents.contains_key(&agent_id)
                    || state.exiting.contains_key(&agent_id)
                    || state.finalizing.contains_key(&agent_id)
                    || !state.live_sharers(&agent_id).is_empty()
                {
                    continue;
                }
                state.sweeping.insert(agent_id.clone());
            }
            #[cfg(test)]
            self.inner.checkouts.before_sweep_removal(&agent_id).await;
            let serial = self.push_lock(&agent_id);
            let removed = {
                let _serial = serial.lock().await;
                self.inner.checkouts.remove(&agent_id).await
            };
            lock(&self.inner.state).sweeping.remove(&agent_id);
            self.inner.swept.notify_waiters();
            // The backend no longer records the agent: nothing may use its
            // secrets.
            self.end_secrets(&agent_id);
            match removed {
                Ok(()) => {
                    tracing::info!(agent = %agent_id, "node: removed the checkout of an agent its backend no longer records");
                    swept.push(agent_id);
                }
                Err(error) => {
                    tracing::warn!(agent = %agent_id, error = %format!("{error:#}"), "node: could not remove an orphaned checkout");
                }
            }
        }
        swept
    }

    /// [`Self::sweep_candidates`] then [`Self::sweep_checkouts`].
    #[cfg(test)]
    pub(super) async fn sweep(&self, lease_agents: &[AgentId]) -> Vec<AgentId> {
        let candidates = self.sweep_candidates(lease_agents);
        self.sweep_checkouts(candidates).await
    }

    /// Ask the agent's provider to cancel its running turn.
    pub(super) async fn cancel(&self, agent_id: &AgentId) {
        let agent = lock(&self.inner.state).agents.get(agent_id).cloned();
        let Some(agent) = agent else { return };
        let ready = agent.ready.borrow().clone();
        if let Some(ready) = ready {
            if let Err(error) =
                intent_acp::session::cancel(&ready.connection, &ready.session_id).await
            {
                let error = agent.redactor.text(error.to_string());
                tracing::info!(agent = %agent_id, %error, "node: cancel was not delivered");
            }
        }
    }

    /// The provider descendants the agent's tracker last saw.
    #[cfg(test)]
    pub(super) fn tracked_descendants(&self, agent_id: &AgentId) -> Vec<i32> {
        lock(&self.inner.state)
            .agents
            .get(agent_id)
            .map(|agent| agent.descendants.pids())
            .unwrap_or_default()
    }

    /// Stop the agent's provider process tree. Its exit is journaled.
    ///
    /// The returned wait resolves once the stop is applied: `stopped` when
    /// the agent was running (or being set up), after its process tree is
    /// killed, its shared slot freed and its exit record journaled;
    /// `not-running` when no such agent runs, once any exit record still
    /// being written for it is journaled. Repeating a stop is harmless.
    pub(super) fn stop(&self, agent_id: &AgentId) -> StopWait {
        // The agent's scripts stop with it.
        let scripts = self.inner.scripts.clone();
        let agent = agent_id.clone();
        // caller-binding: allow — node-local script teardown for a stopped agent
        tokio::spawn(async move { scripts.stop_agent(&agent).await });
        let state = lock(&self.inner.state);
        if let Some(agent) = state.agents.get(agent_id).cloned() {
            drop(state);
            if let Some(stop) = lock(&agent.stop).take() {
                let _ = stop.send(());
            }
            return StopWait::Pending {
                outcome: StopOutcome::Stopped,
                journaled: agent.journaled.clone(),
            };
        }
        match state.exiting.get(agent_id) {
            Some(journaled) => StopWait::Pending {
                outcome: StopOutcome::NotRunning,
                journaled: journaled.clone(),
            },
            None => StopWait::NotRunning,
        }
    }

    /// End every script run, then stop every agent and wait until each
    /// one's last push is done and its queued output and exit are
    /// journaled, and until every final stop's cleanup already under way
    /// has pushed and removed its checkout. All of it is bounded by
    /// [`SHUTDOWN_WAIT`].
    pub(super) async fn shutdown(&self) {
        let started = tokio::time::Instant::now();
        // Close admission first, under the same lock launches are admitted
        // under, so no agent can start after the stop below.
        let running: Vec<AgentId> = {
            let mut state = lock(&self.inner.state);
            state.closing = true;
            state.agents.keys().cloned().collect()
        };
        // An ordinary push under way gives way to each agent's last one.
        self.inner.closing_signal.send_replace(true);
        // Home checkouts stay on disk; their setup and watcher end.
        self.close_homes();
        // Script runs end first (bounded by their reap wait), so nothing
        // still writes to a checkout while its agent's last work is pushed.
        self.inner.scripts.stop_all().await;
        for id in running {
            self.stop(&id);
        }
        let left = SHUTDOWN_WAIT.saturating_sub(started.elapsed());
        let drained = tokio::time::timeout(left, async {
            // A launch admitted just before closing may register its pump
            // after a take, so keep draining until nothing is left.
            loop {
                let pumps = std::mem::take(&mut *lock(&self.inner.pumps));
                let idle = {
                    let state = lock(&self.inner.state);
                    state.agents.is_empty() && state.finalizing.is_empty()
                };
                if pumps.is_empty() {
                    if idle {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                }
                for pump in pumps {
                    let _ = pump.await;
                }
            }
        })
        .await;
        if drained.is_err() {
            tracing::warn!("node: shutdown gave up waiting for agent records to be journaled");
        }
    }
}

/// Which of an agent's pushes runs: a turn's end, or its exit (its last
/// push on this machine).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Push {
    TurnEnd,
    Exit,
}

/// How long a provider whose setup failed is given to exit, so an exit at
/// start is told apart from a setup error.
const EXIT_SETTLE: Duration = Duration::from_millis(500);

enum Outcome {
    Exited(Option<i32>),
    /// The provider exited before its session was ready, with the setup
    /// error it caused, if any.
    ExitedAtStart(Option<i32>, Option<String>),
    Stopped,
    /// Its setup failed; `true` when `initialize` timed out.
    StartFailed(String, bool),
    /// A launch without a profile was refused as not signed in: the
    /// provider id.
    SignedOut(String),
}

impl Outcome {
    /// Whether a second start may succeed: the provider exited before it
    /// was ready, or did not answer `initialize` in time.
    fn start_may_recover(&self) -> bool {
        matches!(self, Self::ExitedAtStart(..) | Self::StartFailed(_, true))
    }
}

/// What every remote agent's first turn on this machine starts with: the
/// long-commands hint as a `<system>` block, then the checkout notices.
pub(super) fn first_turn_preamble(notices: Option<&str>) -> String {
    format!(
        "<system>{}</system>{}",
        intent_core::machine_selection::REMOTE_LONG_COMMANDS_HINT,
        notices.map(|n| format!("\n\n{n}")).unwrap_or_default()
    )
}

/// The provider's [`remote_launch_env`](ProviderConfig::remote_launch_env),
/// applied as Intent wiring on every launch on this machine.
pub(super) fn remote_launch_env(
    config: &ProviderConfig,
) -> impl Iterator<Item = (String, String)> + '_ {
    config
        .remote_launch_env
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
}

/// The most of an exit error [`exit_detail`] keeps.
const EXIT_DETAIL_MAX: usize = 512;

/// Whether a launch without a profile of `provider` is a Default running on
/// this machine's own sign-in, which the user can fix by signing in here or
/// marking a configuration for remote machines.
pub(super) fn signs_in_on_the_machine(provider: &str) -> bool {
    matches!(provider, "claude-code" | "codex")
}

/// The `node-provider-signed-out` start failure: names the provider and the
/// fix, never the provider's own error text (it can echo a credential).
fn signed_out_error(provider: &str) -> String {
    let name = match provider {
        "claude-code" => "Claude Code",
        "codex" => "Codex",
        other => other,
    };
    format!(
        "{}: {name} isn't signed in on this machine, and Default sends no sign-in of its own. Sign in to {name} on this machine, or pick a profile with an API key or token that is marked for remote machines.",
        codes::SIGNED_OUT
    )
}

/// Why an agent exited, as a turn refused after it says: the start failure
/// or exit error the exit record carries, else the provider's status.
fn exit_detail(exit_code: Option<i32>, error: Option<&str>) -> String {
    let error = error.map(|error| {
        let mut end = error.len().min(EXIT_DETAIL_MAX);
        while !error.is_char_boundary(end) {
            end -= 1;
        }
        &error[..end]
    });
    match (error, exit_code) {
        (Some(error), _)
            if error.starts_with(codes::START) || error.starts_with(codes::SIGNED_OUT) =>
        {
            format!("the provider did not start ({error})")
        }
        (Some(error), _) => format!("the agent has exited ({error})"),
        (None, Some(code)) => format!("the provider exited with status {code}"),
        (None, None) => "the agent has exited".to_owned(),
    }
}

/// Load the agent's recorded provider session `session_id`; the history
/// the provider replays is never journaled.
async fn load_recorded(
    agent: &Agent,
    connection: &Connection,
    cwd: &std::path::Path,
    mcp: Vec<intent_acp::session::McpServer>,
    session_id: &str,
) -> Result<(), intent_acp::AcpError> {
    let (ack, acked) = oneshot::channel();
    if agent.marks.send(Mark::Replaying { ack }).is_err() {
        return Err(intent_acp::AcpError::Protocol(
            "the agent's journal is closed".into(),
        ));
    }
    // Cancelled with the load (the provider exited first), the guard still
    // ends the replay, so a retried start's output is journaled.
    let mut replay = ReplayGuard(Some(&agent.marks));
    let _ = acked.await;
    let loaded = intent_acp::session::load_session(connection, session_id, cwd, mcp, None)
        .await
        .map(drop);
    let (ack, acked) = oneshot::channel();
    if replay.end(Some(ack)) {
        let _ = acked.await;
    }
    loaded
}

/// Ends a load's replay on the pump once, when the load finishes or is
/// dropped.
struct ReplayGuard<'a>(Option<&'a mpsc::UnboundedSender<Mark>>);

impl ReplayGuard<'_> {
    fn end(&mut self, ack: Option<oneshot::Sender<()>>) -> bool {
        self.0
            .take()
            .is_some_and(|marks| marks.send(Mark::Replayed { ack }).is_ok())
    }
}

impl Drop for ReplayGuard<'_> {
    fn drop(&mut self) {
        self.end(None);
    }
}

/// How long the provider stays quiet before a load's replay counts as
/// over, and the longest a replay is waited out after the load answered.
const REPLAY_SETTLE: Duration = Duration::from_millis(50);
const REPLAY_SETTLE_CAP: Duration = Duration::from_millis(500);

/// Drop what a load still replays after its answer, until the provider is
/// quiet for [`REPLAY_SETTLE`] or [`REPLAY_SETTLE_CAP`] passed.
async fn settle_replay(notes: &mut mpsc::UnboundedReceiver<IncomingNotification>) {
    let cap = tokio::time::Instant::now() + REPLAY_SETTLE_CAP;
    loop {
        let quiet = (tokio::time::Instant::now() + REPLAY_SETTLE).min(cap);
        match tokio::time::timeout_at(quiet, notes.recv()).await {
            Ok(Some(_)) if tokio::time::Instant::now() < cap => {}
            _ => return,
        }
    }
}

/// Wait for the session, then run one ACP prompt and report its stop reason
/// and any error.
async fn run_turn(agent: &Agent, turn_id: &str, prompt: Vec<Value>) -> (String, Option<String>) {
    let failed = |detail: String| ("refusal".to_owned(), Some(detail));
    let Ok(mut blocks) = prompt
        .into_iter()
        .map(serde_json::from_value)
        .collect::<Result<Vec<_>, _>>()
    else {
        return failed(format!("{}: invalid prompt content", codes::TURN));
    };
    let mut ready = agent.ready.clone();
    let session = match ready.wait_for(Option::is_some).await {
        Ok(session) => session.clone(),
        Err(_) => None,
    };
    let not_running = || {
        let detail = lock(&agent.exit_detail)
            .clone()
            .unwrap_or_else(|| exit_detail(None, None));
        failed(format!("{}: {detail}", codes::NOT_RUNNING))
    };
    let Some(session) = session else {
        return not_running();
    };
    // Set before the agent is ready, so the first turn takes it.
    if let Some(catalog) = lock(&agent.skills_catalog).take() {
        match serde_json::from_value(serde_json::json!({ "type": "text", "text": catalog })) {
            Ok(block) => blocks.insert(0, block),
            Err(error) => {
                tracing::warn!(error = %error, "node: the skills catalog is not a prompt block");
            }
        }
    }
    let notice = lock(&agent.notice).take();
    if let Some(notice) = notice {
        match serde_json::from_value(serde_json::json!({ "type": "text", "text": notice })) {
            Ok(block) => blocks.insert(0, block),
            Err(error) => {
                tracing::warn!(error = %error, "node: the checkout notice is not a prompt block");
            }
        }
    }
    // Switch the pump to this turn before anything the prompt causes can
    // arrive.
    let (ack, switched) = oneshot::channel();
    let started = Mark::TurnStarted {
        turn_id: turn_id.to_owned(),
        ack,
    };
    if agent.marks.send(started).is_err() || switched.await.is_err() {
        return not_running();
    }
    agent.activity.touch();
    match intent_acp::session::prompt(
        &session.connection,
        &session.session_id,
        blocks,
        &agent.activity,
    )
    .await
    {
        Ok(outcome) => (
            serde_json::to_value(outcome.stop_reason)
                .ok()
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_else(|| "end_turn".to_owned()),
            None,
        ),
        Err(error) => {
            agent.note_auth_failure(&error);
            failed(format!("{}: {error}", codes::TURN))
        }
    }
}

/// The pids of processes whose environment carries `RUN_MARKER_ENV=marker`.
/// Linux only (`/proc/<pid>/environ`, readable for the node's own user's
/// processes); elsewhere, and for a helper that clears its environment,
/// only the descendant snapshots find escaped helpers. A recycled pid
/// cannot carry the marker of an earlier run.
#[cfg_attr(not(target_os = "linux"), expect(clippy::unused_async))] // only the linux arm awaits
async fn marked_processes(marker: &str) -> Vec<i32> {
    #[cfg(target_os = "linux")]
    {
        let needle = format!("{RUN_MARKER_ENV}={marker}").into_bytes();
        tokio::task::spawn_blocking(move || {
            let Ok(entries) = std::fs::read_dir("/proc") else {
                return Vec::new();
            };
            entries
                .filter_map(Result::ok)
                .filter_map(|entry| entry.file_name().to_str()?.parse::<i32>().ok())
                .filter(|pid| {
                    std::fs::read(format!("/proc/{pid}/environ"))
                        .is_ok_and(|env| env.split(|b| *b == 0).any(|var| var == needle))
                })
                .collect()
        })
        .await
        .unwrap_or_default()
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = marker;
        Vec::new()
    }
}

/// Stop a provider process tree the way the local agent manager does:
/// SIGTERM its process group, SIGKILL whatever is left after a grace period,
/// then sweep descendants that moved to their own process groups.
/// `spawn_pid` is the pid read at spawn: once the child has been reaped it
/// no longer reports one, but members of its process group may remain (the
/// group id cannot be reused while any member is alive). `known` are
/// descendants seen while it ran: after it exits, helpers in their own
/// process groups are no longer its descendants, so they and their own
/// descendants are swept from that snapshot (and, on Linux, from the run
/// marker).
///
/// Accepted residuals (outside Linux, or for a helper that clears its
/// environment and so carries no run marker), all for a provider that exits
/// by itself:
/// - a helper that leaves the provider's process group after the last
///   snapshot is missed;
/// - a helper started after one snapshot that stops being the provider's
///   descendant (it is reparented, for example when its parent exits)
///   before the next one is in no snapshot, and is missed. Snapshots are
///   published in the order they started ([`Descendants`]), so an older
///   periodic scan can no longer replace the setup snapshot that captured
///   a startup helper.
///
/// Snapshots are deliberately not accumulated: a remembered pid can be
/// reused by an unrelated process, and the pid lists carry no birth
/// identity, so accumulating them risks killing unrelated work, which is
/// worse than leaving a helper behind. Like every pid-based sweep, a pid
/// reused in the instant between a snapshot and the signal could also be
/// signalled; the sweep skips the node's own process group.
async fn kill_tree(mut child: Child, spawn_pid: Option<u32>, known: &[i32]) {
    use nix::sys::signal::{killpg, Signal};
    use nix::unistd::Pid;
    let Some(pid) = child.id().or(spawn_pid) else {
        let _ = child.start_kill();
        let _ = child.wait().await;
        intent_acp::sweep_escaped_descendants(known).await;
        return;
    };
    let mut roots: Vec<u32> = vec![pid];
    roots.extend(known.iter().filter_map(|&p| u32::try_from(p).ok()));
    let mut descendants = intent_acp::descendant_pids_many(&roots).await;
    descendants.extend_from_slice(known);
    descendants.sort_unstable();
    descendants.dedup();
    let group = Pid::from_raw(pid.cast_signed());
    let _ = killpg(group, Signal::SIGTERM);
    let _ = tokio::time::timeout(TERM_GRACE, child.wait()).await;
    let _ = killpg(group, Signal::SIGKILL);
    let _ = child.wait().await;
    intent_acp::sweep_escaped_descendants(&descendants).await;
}

#[cfg(test)]
mod tests;
