//! Where ingested journal records go: one sink per remote agent attached on
//! head, feeding that agent's runtime.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, oneshot, watch, Mutex as TokioMutex};

use intent_acp::transport::IncomingNotification;
use intent_core::node_link::{JournalBody, JournalRecord};
use intent_core::{AgentId, WorkspaceId};

use crate::lock;

/// The shared notification stream an agent runtime hands the manager.
pub(crate) type Notifications = Arc<TokioMutex<mpsc::UnboundedReceiver<IncomingNotification>>>;

/// How a turn ended on the node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TurnEnd {
    pub stop_reason: String,
    pub error: Option<String>,
}

/// How the agent's provider exited on the node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Exit {
    pub exit_code: Option<i32>,
    pub error: Option<String>,
}

#[derive(Default)]
struct SinkState {
    /// Turns a prompt is waiting on, by turn id.
    turns: HashMap<String, oneshot::Sender<TurnEnd>>,
    exit: Option<Exit>,
    /// Head stopped the agent: no new turn waits, none still waiting.
    closed: bool,
}

/// One remote agent's inbox.
pub(crate) struct AgentSink {
    pub workspace_id: WorkspaceId,
    notes_tx: mpsc::UnboundedSender<IncomingNotification>,
    pub notes: Notifications,
    state: Mutex<SinkState>,
    /// Turn ends received; the runtime's response watermark.
    pub responses: watch::Sender<u64>,
    /// Records received that may come from provider side effects (prompts
    /// sent and notifications received); the runtime's request watermark.
    pub requests: AtomicU64,
}

impl AgentSink {
    fn new(workspace_id: WorkspaceId) -> Self {
        let (notes_tx, notes_rx) = mpsc::unbounded_channel();
        Self {
            workspace_id,
            notes_tx,
            notes: Arc::new(TokioMutex::new(notes_rx)),
            state: Mutex::new(SinkState::default()),
            responses: watch::channel(0).0,
            requests: AtomicU64::new(0),
        }
    }

    /// Wait for `turn_id`'s end. The receiver fails when the agent exits.
    /// `None` when it already exited.
    pub(crate) fn expect_turn(&self, turn_id: &str) -> Option<oneshot::Receiver<TurnEnd>> {
        let mut state = lock(&self.state);
        if state.exit.is_some() || state.closed {
            return None;
        }
        let (tx, rx) = oneshot::channel();
        state.turns.insert(turn_id.to_owned(), tx);
        Some(rx)
    }

    /// Stop waiting for `turn_id` (its prompt was dropped or gave up).
    pub(crate) fn forget_turn(&self, turn_id: &str) {
        lock(&self.state).turns.remove(turn_id);
    }

    /// Head stopped the agent: fail every waiting prompt and refuse new
    /// turns.
    pub(crate) fn close(&self) {
        let mut state = lock(&self.state);
        state.closed = true;
        state.turns.clear();
    }

    pub(crate) fn exit(&self) -> Option<Exit> {
        lock(&self.state).exit.clone()
    }

    /// Neither exited on the node nor stopped by head.
    fn is_live(&self) -> bool {
        let state = lock(&self.state);
        state.exit.is_none() && !state.closed
    }

    fn deliver(&self, record: JournalRecord) {
        match record.body {
            // The connector records it on the agent's placement.
            JournalBody::Started { .. } => {}
            JournalBody::Notification { notification } => {
                self.requests.fetch_add(1, Ordering::SeqCst);
                // The manager may have dropped the stream; nothing to do then.
                let _ = self.notes_tx.send(IncomingNotification {
                    method: "session/update".into(),
                    params: notification,
                });
            }
            JournalBody::TurnEnded { stop_reason, error } => {
                let waiter = record
                    .turn_id
                    .as_deref()
                    .and_then(|turn| lock(&self.state).turns.remove(turn));
                if let Some(waiter) = waiter {
                    let _ = waiter.send(TurnEnd { stop_reason, error });
                }
                self.responses.send_modify(|count| *count += 1);
            }
            JournalBody::Exited {
                exit_code, error, ..
            } => {
                let mut state = lock(&self.state);
                state.exit = Some(Exit { exit_code, error });
                // Dropping the waiters fails every pending prompt.
                state.turns.clear();
            }
        }
    }
}

/// Every attached remote agent.
#[derive(Default)]
pub(crate) struct Agents {
    sinks: Mutex<HashMap<AgentId, Arc<AgentSink>>>,
}

impl Agents {
    /// Attach `agent_id`, replacing any earlier sink.
    pub(crate) fn attach(&self, agent_id: &AgentId, workspace_id: WorkspaceId) -> Arc<AgentSink> {
        let sink = Arc::new(AgentSink::new(workspace_id));
        lock(&self.sinks).insert(agent_id.clone(), sink.clone());
        sink
    }

    /// Whether `agent_id` has a sink.
    pub(crate) fn is_attached(&self, agent_id: &AgentId) -> bool {
        lock(&self.sinks).contains_key(agent_id)
    }

    /// Attached agents that neither exited nor were stopped: the node slots
    /// head counts as taken.
    pub(crate) fn live_count(&self) -> usize {
        lock(&self.sinks)
            .values()
            .filter(|sink| sink.is_live())
            .count()
    }

    /// Detach `agent_id` if `sink` is still its sink.
    pub(crate) fn detach(&self, agent_id: &AgentId, sink: &Arc<AgentSink>) {
        let mut sinks = lock(&self.sinks);
        if sinks
            .get(agent_id)
            .is_some_and(|current| Arc::ptr_eq(current, sink))
        {
            sinks.remove(agent_id);
        }
    }

    /// Hand an ingested record to its agent. Records of agents not
    /// attached on head (stopped, or from before a head restart) and records
    /// naming a different workspace than the agent's are dropped.
    pub(crate) fn deliver(&self, record: JournalRecord) {
        let sink = lock(&self.sinks).get(&record.agent_id).cloned();
        let Some(sink) = sink else {
            tracing::debug!(agent = %record.agent_id, seq = record.seq.0, "remote node: record for an agent not attached here; dropped");
            return;
        };
        if sink.workspace_id != record.workspace_id {
            tracing::warn!(agent = %record.agent_id, seq = record.seq.0, "remote node: record names another workspace; dropped");
            return;
        }
        sink.deliver(record);
    }
}
