//! The agent manager's runtime for an agent running on the node.
//!
//! A prompt is a `Prompt` frame tagged with a fresh turn id; the node's
//! journal then delivers the turn's notifications (through
//! [`AgentRuntime::notifications`], which the manager consumes exactly as a
//! local agent's) and its `TurnEnded` record, which completes the prompt.
//! A turn fails when the agent exits, when the link stays down longer than
//! the link-loss grace, or after the same idle window a local prompt has.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use intent_acp::error::JsonRpcError;
use intent_acp::session::{
    prompt_idle_timeout, ActivityTracker, ContentBlock, PromptOutcome, StopReason,
};
use intent_acp::AcpError;
use intent_core::agent_runtime::AgentRuntime;
use intent_core::node_link::LinkFrame;
use intent_core::{AgentId, BoxFuture};

use crate::agents::{AgentSink, Notifications, TurnEnd};
use crate::connector::Shared;

/// JSON-RPC "internal error", for turns the node reports as failed.
const TURN_FAILED_CODE: i64 = -32603;

/// An agent on the configured node, as the agent manager drives it.
pub struct RemoteAgentRuntime {
    agent_id: AgentId,
    sink: Arc<AgentSink>,
    shared: Arc<Shared>,
    stopped: AtomicBool,
}

impl std::fmt::Debug for RemoteAgentRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteAgentRuntime")
            .field("agent_id", &self.agent_id)
            .field("stopped", &self.stopped.load(Ordering::SeqCst))
            .finish_non_exhaustive()
    }
}

fn transport(detail: &str) -> AcpError {
    AcpError::Transport(detail.to_owned())
}

/// Removes the turn's waiter if the prompt is dropped or gives up.
struct TurnGuard<'a> {
    sink: &'a AgentSink,
    turn_id: &'a str,
}

impl Drop for TurnGuard<'_> {
    fn drop(&mut self) {
        self.sink.forget_turn(self.turn_id);
    }
}

impl RemoteAgentRuntime {
    pub(crate) fn new(agent_id: AgentId, sink: Arc<AgentSink>, shared: Arc<Shared>) -> Self {
        Self {
            agent_id,
            sink,
            shared,
            stopped: AtomicBool::new(false),
        }
    }

    /// The agent this runtime drives.
    #[must_use]
    pub fn agent_id(&self) -> &AgentId {
        &self.agent_id
    }

    /// Neither stopped by head nor exited on the node.
    #[must_use]
    pub fn is_running(&self) -> bool {
        !self.stopped.load(Ordering::SeqCst) && self.sink.exit().is_none()
    }

    /// The node's exit report, once the agent's provider exited.
    #[must_use]
    pub fn exit_error(&self) -> Option<String> {
        self.sink
            .exit()
            .map(|exit| match (exit.error, exit.exit_code) {
                (Some(error), _) => error,
                (None, Some(code)) => format!("the remote agent exited with status {code}"),
                (None, None) => "the remote agent exited".to_owned(),
            })
    }

    fn not_running(&self) -> Option<AcpError> {
        if self.stopped.load(Ordering::SeqCst) {
            return Some(transport(
                "node-agent-not-running: the remote agent was stopped",
            ));
        }
        self.exit_error()
            .map(|error| AcpError::Transport(format!("node-agent-not-running: {error}")))
    }

    /// Detach from ingestion and queue the node-side stop; idempotent.
    fn begin_stop(&self) -> Option<tokio::task::JoinHandle<()>> {
        if self.stopped.swap(true, Ordering::SeqCst) {
            return None;
        }
        self.shared.agents.detach(&self.agent_id, &self.sink);
        // Fail prompts already waiting; they would never see a turn end.
        self.sink.close();
        self.shared.request_stop(&self.agent_id)
    }

    async fn run_prompt(
        &self,
        prompt: Vec<ContentBlock>,
        activity: &ActivityTracker,
    ) -> Result<PromptOutcome, AcpError> {
        if let Some(error) = self.not_running() {
            return Err(error);
        }
        let prompt = prompt
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| AcpError::Serde(e.to_string()))?;
        let turn_id = uuid::Uuid::new_v4().to_string();
        let Some(mut ended) = self.sink.expect_turn(&turn_id) else {
            return Err(self
                .not_running()
                .unwrap_or_else(|| transport("node-agent-not-running")));
        };
        let _guard = TurnGuard {
            sink: &self.sink,
            turn_id: &turn_id,
        };
        // A prompt may cause provider side effects; a retry must not repeat it.
        self.sink.requests.fetch_add(1, Ordering::SeqCst);
        // A prompt during a reconnect waits for the link, up to the grace.
        let undelivered =
            || transport("node-link-unavailable: the prompt was not delivered to the node");
        // A stop or an exit drops the turn's waiter, which ends the wait.
        let link = tokio::select! {
            link = self.shared.link_within(self.shared.tuning.link_loss_grace) => {
                link.ok_or_else(undelivered)?
            }
            _ = &mut ended => {
                return Err(self
                    .not_running()
                    .unwrap_or_else(|| transport("node-agent-not-running")));
            }
        };
        // The agent may have been stopped just as the link came up.
        if let Some(error) = self.not_running() {
            return Err(error);
        }
        link.send(&LinkFrame::Prompt {
            agent_id: self.agent_id.clone(),
            turn_id: turn_id.clone(),
            prompt,
        })
        .await
        .map_err(|_| undelivered())?;
        activity.touch();
        let idle_window = prompt_idle_timeout();
        let mut down_since: Option<Instant> = None;
        loop {
            tokio::select! {
                end = &mut ended => {
                    return match end {
                        Ok(end) => outcome(end),
                        Err(_) => Err(self
                            .not_running()
                            .unwrap_or_else(|| transport("node-agent-not-running"))),
                    };
                }
                () = tokio::time::sleep(self.shared.tuning.turn_poll) => {
                    let idle = Duration::from_millis(activity.idle_ms());
                    if idle >= idle_window {
                        return Err(AcpError::PromptIdleTimeout(idle_window));
                    }
                    if self.shared.current_link().is_some() {
                        down_since = None;
                    } else {
                        let since = *down_since.get_or_insert_with(Instant::now);
                        if since.elapsed() >= self.shared.tuning.link_loss_grace {
                            return Err(transport(
                                "node-link-unavailable: the link to the node was lost during the turn",
                            ));
                        }
                    }
                }
            }
        }
    }
}

fn outcome(end: TurnEnd) -> Result<PromptOutcome, AcpError> {
    if let Some(error) = end.error {
        return Err(AcpError::Rpc(JsonRpcError {
            code: TURN_FAILED_CODE,
            message: error,
            data: None,
        }));
    }
    let stop_reason =
        serde_json::from_value::<StopReason>(serde_json::Value::String(end.stop_reason))
            .unwrap_or(StopReason::EndTurn);
    Ok(PromptOutcome {
        stop_reason,
        usage: None,
        meta: None,
    })
}

impl AgentRuntime for RemoteAgentRuntime {
    type Prompt = Vec<ContentBlock>;
    type PromptOutcome = PromptOutcome;
    type Activity = ActivityTracker;
    type Error = AcpError;
    type Notifications = Notifications;

    fn prompt<'a>(
        &'a self,
        _session_id: &'a str,
        prompt: Self::Prompt,
        activity: &'a ActivityTracker,
    ) -> BoxFuture<'a, Result<PromptOutcome, AcpError>> {
        Box::pin(self.run_prompt(prompt, activity))
    }

    fn cancel<'a>(&'a self, _session_id: &'a str) -> BoxFuture<'a, Result<(), AcpError>> {
        Box::pin(async move {
            if let Some(error) = self.not_running() {
                return Err(error);
            }
            self.shared
                .send(&LinkFrame::Cancel {
                    agent_id: self.agent_id.clone(),
                })
                .await
                .map_err(|_| {
                    transport("node-link-unavailable: the cancel was not delivered to the node")
                })
        })
    }

    fn notifications(&self) -> Notifications {
        self.sink.notes.clone()
    }

    fn is_alive(&self) -> bool {
        !self.stopped.load(Ordering::SeqCst) && self.sink.exit().is_none()
    }

    fn client_request_seq(&self) -> u64 {
        self.sink.requests.load(Ordering::SeqCst)
    }

    fn response_seq(&self) -> u64 {
        *self.sink.responses.borrow()
    }

    fn await_response_after(&self, since: u64, timeout: Duration) -> BoxFuture<'_, bool> {
        let mut responses = self.sink.responses.subscribe();
        Box::pin(async move {
            tokio::time::timeout(timeout, responses.wait_for(|count| *count > since))
                .await
                .is_ok_and(|seen| seen.is_ok())
        })
    }

    fn spawned_pid(&self) -> Option<u32> {
        None
    }

    fn root_pid(&self) -> Option<u32> {
        None
    }

    fn stop(&self) -> BoxFuture<'static, ()> {
        let sent = self.begin_stop();
        Box::pin(async move {
            if let Some(sent) = sent {
                let _ = sent.await;
            }
        })
    }
}

impl Drop for RemoteAgentRuntime {
    fn drop(&mut self) {
        drop(self.begin_stop());
    }
}
