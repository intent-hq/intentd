//! Runs head's agents on the node: an in-memory capacity counter, provider
//! processes started with `intent_acp::spawn_provider` and driven over the
//! local ACP client, and a journal record for everything head must see.
//!
//! Head learns about an agent only through the journal: provider
//! `session/update` notifications, turn ends and process exits (including a
//! launch that failed or was refused for capacity). Records of one agent are
//! journaled in order by a single per-agent pump.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use tokio::process::Child;
use tokio::sync::{mpsc, oneshot, watch};

use intent_acp::session::ActivityTracker;
use intent_acp::{
    ClientRequestHandler, Connection, ConnectionHooks, EventSink, FileService,
    IncomingNotification, IncomingRequest, PermissionPolicy, PermissionRegistry, ProfileEnv,
    SinkEvent, SpawnOptions,
};
use intent_core::node_link::{JournalBody, JournalRecord, LaunchCommand, LeaseBinding};
use intent_core::nodes::NodeCounter;
use intent_core::{AgentId, BoxFuture, WorkspaceId};
use intent_nodes::journal::{Journal, JournalEntry};
use intent_providers::ProviderConfig;

use super::checkout::Checkouts;

/// Error codes at the start of a journaled launch or turn error.
pub(super) mod codes {
    /// The node is running `maxAgents` agents already.
    pub const CAPACITY: &str = "node-capacity";
    /// The provider is not configured on this node.
    pub const PROVIDER: &str = "node-provider-unavailable";
    /// The provider process could not be started or set up.
    pub const START: &str = "node-agent-start-failed";
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
/// How long shutdown waits for stopped agents to be cleaned up and their
/// queued records (each one fsync'd) to be journaled.
const SHUTDOWN_WAIT: Duration = Duration::from_secs(30);

/// A provider the node may launch: its registry entry (with any configured
/// argument override) and the configured executable.
#[derive(Debug, Clone)]
pub(super) struct ProviderLaunch {
    pub config: ProviderConfig,
    pub binary: PathBuf,
}

/// What the runner needs.
pub(super) struct RunnerConfig {
    pub max_agents: u32,
    pub policy: PermissionPolicy,
    pub providers: BTreeMap<String, ProviderLaunch>,
    pub checkouts: Arc<dyn Checkouts>,
    pub journal: Journal,
    /// The node user's home: a leading `~` in a profile's account directory
    /// expands against it.
    pub home: Option<PathBuf>,
}

/// The node's agents. Cloning shares them.
#[derive(Clone)]
pub(super) struct Runner {
    inner: Arc<Inner>,
}

struct Inner {
    max_agents: usize,
    policy: PermissionPolicy,
    providers: BTreeMap<String, ProviderLaunch>,
    checkouts: Arc<dyn Checkouts>,
    home: Option<PathBuf>,
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
}

struct Agent {
    id: AgentId,
    workspace_id: WorkspaceId,
    /// The commit head launched the agent at, and the checkout cache key.
    base_commit: String,
    repo_key: String,
    /// The agent's working directory, once prepared.
    worktree: Mutex<Option<PathBuf>>,
    /// Serializes the agent's WIP pushes.
    wip: tokio::sync::Mutex<()>,
    stop: Mutex<Option<oneshot::Sender<()>>>,
    ready: watch::Receiver<Option<Arc<Ready>>>,
    /// Whether a turn is running; refuses a second one.
    busy: Mutex<bool>,
    marks: mpsc::UnboundedSender<Mark>,
    activity: ActivityTracker,
    /// The provider's descendants at the last snapshot.
    descendants: Arc<Mutex<Vec<i32>>>,
    /// This run's [`RUN_MARKER_ENV`] value.
    run_marker: String,
    /// Scrubs the profile's secret values from what the agent journals.
    redactor: Arc<Redactor>,
}

/// Replaces a launch profile's secret values with [`Redactor::MASK`] in the
/// provider-originated text the node journals or logs (session updates and
/// provider error messages, which can echo a rejected credential). Every
/// value of a credential-named variable (`*_API_KEY`, `*_TOKEN`, `*SECRET*`,
/// `*PASSWORD*`, ...; [`intent_acp::is_likely_secret_env_key`]) is masked
/// whatever its length; another variable's value is masked from
/// [`Redactor::MIN_LEN`] bytes, so a short setting such as `1` does not
/// scrub ordinary text. Account-folder paths are not secrets.
#[derive(Default)]
struct Redactor {
    /// Longest first, so a secret containing another is masked whole.
    secrets: Vec<String>,
}

impl Redactor {
    const MASK: &'static str = "[redacted]";
    const MIN_LEN: usize = 8;

    fn new(profile: Option<&ProfileEnv>) -> Self {
        let mut secrets: Vec<String> = profile
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
                    .then(|| value.to_owned())
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

    /// Mask secrets anywhere in arbitrary provider data (tool input and
    /// output, metadata, prose): every string and every object key.
    fn value(&self, value: &mut Value) {
        if self.secrets.is_empty() {
            return;
        }
        match value {
            Value::String(text) => *text = self.text(std::mem::take(text)),
            Value::Array(items) => items.iter_mut().for_each(|item| self.value(item)),
            Value::Object(map) => {
                let entries = std::mem::take(map);
                *map = entries
                    .into_iter()
                    .map(|(key, mut item)| {
                        self.value(&mut item);
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
    /// tool-content tags, plan entry status/priority) are kept as sent, so
    /// head can still parse the update even when a short secret equals a
    /// protocol word; everything else, including tool `rawInput` /
    /// `rawOutput` at any depth, is masked with [`Self::value`].
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
            _ => Keep::Mask,
        });
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
                Keep::Id | Keep::Mask => self.value(item),
            }
        }
    }
}

/// What [`Redactor::schema`] does with one field.
enum Keep {
    /// A protocol identifier: kept as sent when it is a string.
    Id,
    /// A nested schema object.
    Walk(fn(&Redactor, &mut Value)),
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
        Self {
            inner: Arc::new(Inner {
                max_agents: config.max_agents as usize,
                policy: config.policy,
                providers: config.providers,
                checkouts: config.checkouts,
                home: config.home,
                journal: Arc::new(Mutex::new(config.journal)),
                appended: watch::channel(last).0,
                state: Mutex::new(State::default()),
                pumps: Mutex::new(Vec::new()),
                link_gate: tokio::sync::Mutex::new(None),
            }),
        }
    }

    pub(super) fn max_agents(&self) -> u32 {
        u32::try_from(self.inner.max_agents).unwrap_or(u32::MAX)
    }

    /// The agents running or starting, for the welcome.
    pub(super) fn running(&self) -> Vec<AgentId> {
        let mut ids: Vec<AgentId> = lock(&self.inner.state).agents.keys().cloned().collect();
        ids.sort_by(|a, b| a.0.cmp(&b.0));
        ids
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
        if let Some(refusal) = self.begin_launch(command) {
            self.append(refusal).await;
        }
    }

    /// Admit and start a launch, or return the refusal to journal.
    fn begin_launch(&self, command: LaunchCommand) -> Option<JournalEntry> {
        let LaunchCommand {
            agent_id,
            workspace_id,
            provider,
            model,
            base_commit,
            repo_key,
            profile_env,
        } = command;
        let exited = |error: String| JournalEntry {
            agent_id: agent_id.clone(),
            workspace_id: workspace_id.clone(),
            turn_id: None,
            recorded_at: intent_core::now_iso(),
            body: JournalBody::Exited {
                exit_code: None,
                error: Some(error),
            },
        };
        let mut state = lock(&self.inner.state);
        if state.closing {
            drop(state);
            return Some(exited(format!(
                "{}: the node is shutting down",
                codes::SHUTTING_DOWN
            )));
        }
        if state.agents.contains_key(&agent_id) {
            // The running agent's own records are what head should see; a
            // duplicate launch is not journaled as its exit.
            tracing::warn!(agent = %agent_id, "node: launch for an agent that is already running");
            return None;
        }
        if state.agents.len() >= self.inner.max_agents {
            let running = state.agents.len();
            drop(state);
            return Some(exited(format!(
                "{}: this node is running {running} of {} agents",
                codes::CAPACITY,
                self.inner.max_agents
            )));
        }
        let Some(launch) = self.inner.providers.get(&provider).cloned() else {
            drop(state);
            let configured: Vec<&str> = self.inner.providers.keys().map(String::as_str).collect();
            return Some(exited(format!(
                "{}: provider {provider:?} is not configured on this node (configured: {})",
                codes::PROVIDER,
                configured.join(", ")
            )));
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
        let (stop_tx, stop_rx) = oneshot::channel();
        let (ready_tx, ready_rx) = watch::channel(None);
        let (marks_tx, marks_rx) = mpsc::unbounded_channel();
        let (notes_tx, notes_rx) = mpsc::unbounded_channel();
        let agent = Arc::new(Agent {
            id: agent_id.clone(),
            workspace_id: workspace_id.clone(),
            base_commit,
            repo_key,
            worktree: Mutex::new(None),
            wip: tokio::sync::Mutex::new(()),
            stop: Mutex::new(Some(stop_tx)),
            ready: ready_rx,
            busy: Mutex::new(false),
            descendants: Arc::new(Mutex::new(Vec::new())),
            run_marker: intent_core::node_link::LinkId::new_v4().to_string(),
            marks: marks_tx,
            activity: ActivityTracker::new(),
            redactor: Arc::new(Redactor::new(profile.as_ref())),
        });
        state.agents.insert(agent_id.clone(), agent.clone());
        state.workspaces.insert(agent_id, workspace_id);
        drop(state);
        // caller-binding: allow — node-local journal pump; the node has no service layer
        let pump = tokio::spawn(self.clone().pump(
            (agent.id.clone(), agent.workspace_id.clone()),
            (agent.activity.clone(), agent.redactor.clone()),
            notes_rx,
            marks_rx,
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
    ) {
        let mut turn: Option<String> = None;
        loop {
            let (turn_id, body) = tokio::select! {
                biased;
                Some(note) = notes.recv() => {
                    activity.touch();
                    if note.method != "session/update" {
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
                    Mark::TurnEnded { turn_id, stop_reason, error } => {
                        turn = None;
                        let error = error.map(|error| redactor.text(error));
                        (Some(turn_id), JournalBody::TurnEnded { stop_reason, error })
                    }
                    Mark::Exited { exit_code, error } => {
                        let error = error.map(|error| redactor.text(error));
                        (None, JournalBody::Exited { exit_code, error })
                    }
                },
                else => break,
            };
            self.append(JournalEntry {
                agent_id: agent_id.clone(),
                workspace_id: workspace_id.clone(),
                turn_id,
                recorded_at: intent_core::now_iso(),
                body,
            })
            .await;
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
        let prepared = tokio::select! {
            prepared = self.inner.checkouts.prepare(
                &agent.id,
                &agent.workspace_id,
                &agent.base_commit,
                &agent.repo_key,
            ) => Some(prepared),
            _ = &mut stop => None,
        };
        let started = match prepared {
            None => Err(None),
            Some(Err(error)) => Err(Some(format!("{}: {error:#}", codes::CHECKOUT))),
            Some(Ok(cwd)) => {
                *lock(&agent.worktree) = Some(cwd.clone());
                self.start(
                    &agent,
                    (&launch, profile.as_ref()),
                    model.as_deref(),
                    notes,
                    cwd,
                )
                .map_err(|error| Some(format!("{}: {error}", codes::START)))
            }
        };
        let (exit_code, error) = match started {
            Err(error) => (None, error),
            Ok((mut child, connection, serve, cwd)) => {
                // Read before waiting: a reaped child no longer reports it,
                // and its process group can outlive it.
                let pid = child.id();
                let tracker = pid.map(|pid| {
                    let descendants = agent.descendants.clone();
                    // caller-binding: allow — node-local process snapshot; the node has no service layer
                    tokio::spawn(async move {
                        let mut tick = tokio::time::interval(TRACK_INTERVAL);
                        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                        loop {
                            tick.tick().await;
                            let found = intent_acp::descendant_pids(pid).await;
                            *lock(&descendants) = found;
                        }
                    })
                });
                let outcome = self
                    .supervise(
                        &agent,
                        &launch,
                        &connection,
                        &mut child,
                        &mut stop,
                        &ready,
                        &cwd,
                        (pid, &agent.descendants),
                    )
                    .await;
                if let Some(tracker) = tracker {
                    tracker.abort();
                }
                // Whatever the provider left behind goes too, also when it
                // exited by itself: its process group, the helpers last seen
                // as its descendants (and theirs), and on Linux every process
                // still carrying this run's marker.
                let mut known = lock(&agent.descendants).clone();
                known.extend(marked_processes(&agent.run_marker).await);
                kill_tree(child, pid, &known).await;
                let result = match outcome {
                    Outcome::Exited(code) => (code, None),
                    Outcome::Stopped => (None, None),
                    Outcome::StartFailed(error) => {
                        (None, Some(format!("{}: {error}", codes::START)))
                    }
                };
                serve.abort();
                result
            }
        };
        // Turns still waiting for the session see it is gone.
        drop(ready);
        // Whatever the agent did is sent back before head hears it exited.
        self.push_wip(&agent).await;
        {
            let mut state = lock(&self.inner.state);
            if state
                .agents
                .get(&agent.id)
                .is_some_and(|current| Arc::ptr_eq(current, &agent))
            {
                state.agents.remove(&agent.id);
            }
        }
        let _ = agent.marks.send(Mark::Exited { exit_code, error });
    }

    fn start(
        &self,
        agent: &Agent,
        (launch, profile): (&ProviderLaunch, Option<&ProfileEnv>),
        model: Option<&str>,
        notes: mpsc::UnboundedSender<IncomingNotification>,
        cwd: PathBuf,
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
        options.provider_binary = Some(&launch.binary);
        options.model = model;
        options.profile_env = profile;
        options
            .extra_env
            .insert(RUN_MARKER_ENV.to_owned(), agent.run_marker.clone());
        let spawned = intent_acp::spawn_provider(&options, hooks)?;
        // A configured binary launches directly, never through npx, so there
        // is no npx launch directory to keep alive.
        let (child, connection, _npx_launch_dir) = spawned.into_parts();
        let connection = Arc::new(connection);
        let handler = ClientRequestHandler::new(
            agent.workspace_id.clone(),
            agent.id.clone(),
            agent.id.0.clone(),
            FileService::new(&cwd),
            Arc::new(PermissionRegistry::new()),
            self.inner.policy,
            Arc::new(NodeSink),
        );
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

    #[expect(
        clippy::too_many_arguments,
        reason = "private step of run_agent over its locals"
    )]
    async fn supervise(
        &self,
        agent: &Agent,
        launch: &ProviderLaunch,
        connection: &Arc<Connection>,
        child: &mut Child,
        stop: &mut oneshot::Receiver<()>,
        ready: &watch::Sender<Option<Arc<Ready>>>,
        cwd: &std::path::Path,
        (pid, descendants): (Option<u32>, &Mutex<Vec<i32>>),
    ) -> Outcome {
        let setup = async {
            intent_acp::handshake(connection, &launch.config).await?;
            intent_acp::session::new_session(connection, cwd, Vec::new(), None).await
        };
        let session = tokio::select! {
            result = setup => match result {
                Ok(session) => session,
                Err(error) => return Outcome::StartFailed(error.to_string()),
            },
            status = child.wait() => return Outcome::Exited(status.ok().and_then(|s| s.code())),
            _ = &mut *stop => return Outcome::Stopped,
        };
        // Record what the provider started during setup before any prompt
        // can make it exit: once it has exited, helpers in their own process
        // groups are no longer its descendants.
        if let Some(pid) = pid {
            let found = intent_acp::descendant_pids(pid).await;
            *lock(descendants) = found;
        }
        let _ = agent.marks.send(Mark::Started {
            base_commit: agent.base_commit.clone(),
        });
        ready.send_replace(Some(Arc::new(Ready {
            connection: connection.clone(),
            session_id: session.session_id.to_string(),
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
            agent_id: agent_id.clone(),
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
            return workspace
                .map(|workspace| refuse(workspace, codes::NOT_RUNNING, "the agent has exited"));
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
            runner.push_wip(&agent).await;
        });
        None
    }

    /// Push the agent's work to head, if its checkout was prepared. Failures
    /// are logged: the next push sends the same refs again.
    async fn push_wip(&self, agent: &Agent) {
        let Some(worktree) = lock(&agent.worktree).clone() else {
            return;
        };
        let _serial = agent.wip.lock().await;
        if let Err(error) = self
            .inner
            .checkouts
            .push_wip(&agent.id, &agent.workspace_id, &worktree)
            .await
        {
            tracing::warn!(agent = %agent.id, error = %format!("{error:#}"), "node: WIP push failed");
        }
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
            .map(|agent| lock(&agent.descendants).clone())
            .unwrap_or_default()
    }

    /// Stop the agent's provider process tree. Its exit is journaled.
    pub(super) fn stop(&self, agent_id: &AgentId) {
        let agent = lock(&self.inner.state).agents.get(agent_id).cloned();
        if let Some(agent) = agent {
            if let Some(stop) = lock(&agent.stop).take() {
                let _ = stop.send(());
            }
        }
    }

    /// Stop every agent and wait (bounded) until each one's queued output
    /// and exit are journaled.
    pub(super) async fn shutdown(&self) {
        // Close admission first, under the same lock launches are admitted
        // under, so no agent can start after the stop below.
        let running: Vec<AgentId> = {
            let mut state = lock(&self.inner.state);
            state.closing = true;
            state.agents.keys().cloned().collect()
        };
        for id in running {
            self.stop(&id);
        }
        let drained = tokio::time::timeout(SHUTDOWN_WAIT, async {
            // A launch admitted just before closing may register its pump
            // after a take, so keep draining until nothing is left.
            loop {
                let pumps = std::mem::take(&mut *lock(&self.inner.pumps));
                let idle = lock(&self.inner.state).agents.is_empty();
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

enum Outcome {
    Exited(Option<i32>),
    Stopped,
    StartFailed(String),
}

/// Wait for the session, then run one ACP prompt and report its stop reason
/// and any error.
async fn run_turn(agent: &Agent, turn_id: &str, prompt: Vec<Value>) -> (String, Option<String>) {
    let failed = |detail: String| ("refusal".to_owned(), Some(detail));
    let Ok(blocks) = prompt
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
    let Some(session) = session else {
        return failed(format!("{}: the agent has exited", codes::NOT_RUNNING));
    };
    // Switch the pump to this turn before anything the prompt causes can
    // arrive.
    let (ack, switched) = oneshot::channel();
    let started = Mark::TurnStarted {
        turn_id: turn_id.to_owned(),
        ack,
    };
    if agent.marks.send(started).is_err() || switched.await.is_err() {
        return failed(format!("{}: the agent has exited", codes::NOT_RUNNING));
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
        Err(error) => failed(format!("{}: {error}", codes::TURN)),
    }
}

/// The pids of processes whose environment carries `RUN_MARKER_ENV=marker`.
/// Linux only (`/proc/<pid>/environ`, readable for the node's own user's
/// processes); elsewhere, and for a helper that clears its environment,
/// only the descendant snapshots find escaped helpers. A recycled pid
/// cannot carry the marker of an earlier run.
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
/// Accepted v1 residuals (outside Linux, or for a helper that clears its
/// environment and so carries no run marker), all for a provider that exits
/// by itself:
/// - a helper that leaves the provider's process group after the last
///   snapshot is missed;
/// - each snapshot replaces the previous one, so a periodic snapshot taken
///   before a startup helper existed can, if it completes last, replace the
///   setup snapshot that captured it, and that helper is then missed.
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
