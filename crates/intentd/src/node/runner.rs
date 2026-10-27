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
    IncomingNotification, IncomingRequest, PermissionPolicy, PermissionRegistry, SinkEvent,
    SpawnOptions,
};
use intent_core::node_link::{JournalBody, JournalRecord, LaunchCommand};
use intent_core::nodes::NodeCounter;
use intent_core::{AgentId, BoxFuture, WorkspaceId};
use intent_nodes::journal::{Journal, JournalEntry};
use intent_providers::ProviderConfig;

use super::storage::Storage;

/// Error codes at the start of a journaled launch or turn error.
pub(super) mod codes {
    /// The node is running `maxAgents` agents already.
    pub const CAPACITY: &str = "node-capacity";
    /// The provider is not configured on this node.
    pub const PROVIDER: &str = "node-provider-unavailable";
    /// The provider process could not be started or set up.
    pub const START: &str = "node-agent-start-failed";
    /// The agent is not running on this node.
    pub const NOT_RUNNING: &str = "node-agent-not-running";
    /// The agent already has a turn running.
    pub const BUSY: &str = "node-agent-busy";
    /// The turn failed.
    pub const TURN: &str = "node-turn-failed";
}

/// How long a stopped provider gets to exit after SIGTERM before SIGKILL.
const TERM_GRACE: Duration = Duration::from_secs(2);
/// How long shutdown waits for stopped agents to be cleaned up.
const SHUTDOWN_WAIT: Duration = Duration::from_secs(10);

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
    pub storage: Arc<Storage>,
    pub journal: Journal,
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
    storage: Arc<Storage>,
    journal: Arc<Mutex<Journal>>,
    appended: watch::Sender<NodeCounter>,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// Running (or starting) agents; their count is the capacity in use.
    agents: HashMap<AgentId, Arc<Agent>>,
    /// The workspace of every agent launched since start, so a prompt for an
    /// agent that has exited can still be answered with a journal record.
    workspaces: HashMap<AgentId, WorkspaceId>,
}

struct Agent {
    id: AgentId,
    workspace_id: WorkspaceId,
    stop: Mutex<Option<oneshot::Sender<()>>>,
    ready: watch::Receiver<Option<Arc<Ready>>>,
    /// The running turn, if any; shared with the pump, which tags
    /// notifications with it.
    turn: Arc<Mutex<Option<String>>>,
    marks: mpsc::UnboundedSender<Mark>,
    activity: ActivityTracker,
}

/// A provider session that finished its ACP setup.
struct Ready {
    connection: Arc<Connection>,
    session_id: String,
}

/// Records the agent's pump journals after every notification already
/// received.
enum Mark {
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
                storage: config.storage,
                journal: Arc::new(Mutex::new(config.journal)),
                appended: watch::channel(last).0,
                state: Mutex::new(State::default()),
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

    fn record_detached(&self, entry: JournalEntry) {
        let this = self.clone();
        // caller-binding: allow — node-local journal write; the node has no service layer
        tokio::spawn(async move {
            this.append(entry).await;
        });
    }

    /// Start `command`'s agent. A refusal (capacity, unknown provider) or a
    /// failed start is journaled as the agent's exit with an error code.
    pub(super) fn launch(&self, command: LaunchCommand) {
        let LaunchCommand {
            agent_id,
            workspace_id,
            provider,
            model,
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
        if state.agents.contains_key(&agent_id) {
            // The running agent's own records are what head should see; a
            // duplicate launch is not journaled as its exit.
            tracing::warn!(agent = %agent_id, "node: launch for an agent that is already running");
            return;
        }
        if state.agents.len() >= self.inner.max_agents {
            let running = state.agents.len();
            drop(state);
            self.record_detached(exited(format!(
                "{}: this node is running {running} of {} agents",
                codes::CAPACITY,
                self.inner.max_agents
            )));
            return;
        }
        let Some(launch) = self.inner.providers.get(&provider).cloned() else {
            drop(state);
            let configured: Vec<&str> = self.inner.providers.keys().map(String::as_str).collect();
            self.record_detached(exited(format!(
                "{}: provider {provider:?} is not configured on this node (configured: {})",
                codes::PROVIDER,
                configured.join(", ")
            )));
            return;
        };
        let (stop_tx, stop_rx) = oneshot::channel();
        let (ready_tx, ready_rx) = watch::channel(None);
        let (marks_tx, marks_rx) = mpsc::unbounded_channel();
        let (notes_tx, notes_rx) = mpsc::unbounded_channel();
        let agent = Arc::new(Agent {
            id: agent_id.clone(),
            workspace_id: workspace_id.clone(),
            stop: Mutex::new(Some(stop_tx)),
            ready: ready_rx,
            turn: Arc::new(Mutex::new(None)),
            marks: marks_tx,
            activity: ActivityTracker::new(),
        });
        state.agents.insert(agent_id.clone(), agent.clone());
        state.workspaces.insert(agent_id, workspace_id);
        drop(state);
        // caller-binding: allow — node-local journal pump; the node has no service layer
        tokio::spawn(self.clone().pump(
            (agent.id.clone(), agent.workspace_id.clone()),
            agent.turn.clone(),
            agent.activity.clone(),
            notes_rx,
            marks_rx,
        ));
        // caller-binding: allow — node-local provider process; the node has no service layer
        tokio::spawn(
            self.clone()
                .run_agent(agent, launch, model, notes_tx, ready_tx, stop_rx),
        );
    }

    /// Journal the agent's notifications and marks in arrival order. A mark
    /// is taken only once every notification already queued is journaled,
    /// so a turn end never precedes that turn's output. The pump ends once
    /// the provider connection and every holder of the agent are gone, so a
    /// turn that ends after the process exited is still journaled.
    async fn pump(
        self,
        (agent_id, workspace_id): (AgentId, WorkspaceId),
        turn: Arc<Mutex<Option<String>>>,
        activity: ActivityTracker,
        mut notes: mpsc::UnboundedReceiver<IncomingNotification>,
        mut marks: mpsc::UnboundedReceiver<Mark>,
    ) {
        loop {
            let (turn_id, body) = tokio::select! {
                biased;
                Some(note) = notes.recv() => {
                    activity.touch();
                    if note.method != "session/update" {
                        continue;
                    }
                    let turn = lock(&turn).clone();
                    (turn, JournalBody::Notification { notification: note.params })
                }
                Some(mark) = marks.recv() => match mark {
                    Mark::TurnEnded { turn_id, stop_reason, error } => {
                        (Some(turn_id), JournalBody::TurnEnded { stop_reason, error })
                    }
                    Mark::Exited { exit_code, error } => {
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
        launch: ProviderLaunch,
        model: Option<String>,
        notes: mpsc::UnboundedSender<IncomingNotification>,
        ready: watch::Sender<Option<Arc<Ready>>>,
        mut stop: oneshot::Receiver<()>,
    ) {
        let (exit_code, error) = match self.start(&agent, &launch, model.as_deref(), notes) {
            Err(error) => (None, Some(format!("{}: {error}", codes::START))),
            Ok((mut child, connection, serve, cwd)) => {
                let outcome = self
                    .supervise(&launch, &connection, &mut child, &mut stop, &ready, &cwd)
                    .await;
                let result = match outcome {
                    Outcome::Exited(code) => (code, None),
                    Outcome::Stopped => {
                        kill_tree(child).await;
                        (None, None)
                    }
                    Outcome::StartFailed(error) => {
                        kill_tree(child).await;
                        (None, Some(format!("{}: {error}", codes::START)))
                    }
                };
                serve.abort();
                result
            }
        };
        // Turns still waiting for the session see it is gone.
        drop(ready);
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
        launch: &ProviderLaunch,
        model: Option<&str>,
        notes: mpsc::UnboundedSender<IncomingNotification>,
    ) -> anyhow::Result<(Child, Arc<Connection>, tokio::task::JoinHandle<()>, PathBuf)> {
        let cwd = self.inner.storage.agent_dir(&agent.id)?;
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

    async fn supervise(
        &self,
        launch: &ProviderLaunch,
        connection: &Arc<Connection>,
        child: &mut Child,
        stop: &mut oneshot::Receiver<()>,
        ready: &watch::Sender<Option<Arc<Ready>>>,
        cwd: &std::path::Path,
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
        ready.send_replace(Some(Arc::new(Ready {
            connection: connection.clone(),
            session_id: session.session_id.to_string(),
        })));
        tokio::select! {
            status = child.wait() => Outcome::Exited(status.ok().and_then(|s| s.code())),
            _ = &mut *stop => Outcome::Stopped,
        }
    }

    /// Run a turn. Its output streams as notifications; its end (or why it
    /// could not run) is journaled as a turn end.
    pub(super) fn prompt(&self, agent_id: &AgentId, turn_id: String, prompt: Vec<Value>) {
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
            if let Some(workspace) = workspace {
                self.record_detached(refuse(
                    workspace,
                    codes::NOT_RUNNING,
                    "the agent has exited",
                ));
            } else {
                tracing::warn!(agent = %agent_id, "node: prompt for an unknown agent");
            }
            return;
        };
        {
            let mut turn = lock(&agent.turn);
            if turn.is_some() {
                drop(turn);
                self.record_detached(refuse(
                    agent.workspace_id.clone(),
                    codes::BUSY,
                    "a turn is already running",
                ));
                return;
            }
            *turn = Some(turn_id.clone());
        }
        // caller-binding: allow — node-local ACP turn; the node has no service layer
        tokio::spawn(async move {
            let (stop_reason, error) = run_turn(&agent, prompt).await;
            let _ = agent.marks.send(Mark::TurnEnded {
                turn_id,
                stop_reason,
                error,
            });
            *lock(&agent.turn) = None;
        });
    }

    /// Ask the agent's provider to cancel its running turn.
    pub(super) async fn cancel(&self, agent_id: &AgentId) {
        let ready = lock(&self.inner.state)
            .agents
            .get(agent_id)
            .and_then(|agent| agent.ready.borrow().clone());
        if let Some(ready) = ready {
            if let Err(error) =
                intent_acp::session::cancel(&ready.connection, &ready.session_id).await
            {
                tracing::info!(agent = %agent_id, %error, "node: cancel was not delivered");
            }
        }
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

    /// Stop every agent and wait (bounded) for their cleanup.
    pub(super) async fn shutdown(&self) {
        for id in self.running() {
            self.stop(&id);
        }
        let deadline = tokio::time::Instant::now() + SHUTDOWN_WAIT;
        while !lock(&self.inner.state).agents.is_empty() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
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
async fn run_turn(agent: &Agent, prompt: Vec<Value>) -> (String, Option<String>) {
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

/// Stop a provider process tree the way the local agent manager does:
/// SIGTERM its process group, SIGKILL whatever is left after a grace period,
/// then sweep descendants that moved to their own process groups.
async fn kill_tree(mut child: Child) {
    use nix::sys::signal::{killpg, Signal};
    use nix::unistd::Pid;
    let Some(pid) = child.id() else {
        let _ = child.start_kill();
        let _ = child.wait().await;
        return;
    };
    let descendants = intent_acp::descendant_pids(pid).await;
    let group = Pid::from_raw(pid.cast_signed());
    let _ = killpg(group, Signal::SIGTERM);
    let _ = tokio::time::timeout(TERM_GRACE, child.wait()).await;
    let _ = killpg(group, Signal::SIGKILL);
    let _ = child.wait().await;
    intent_acp::sweep_escaped_descendants(&descendants).await;
}

#[cfg(test)]
mod tests;
