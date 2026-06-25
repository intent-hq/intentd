//! Agent session driver: drive an ACP turn and route streaming updates onto the
//! M2 event bus (§6.5/§6.6).
//!
//! `intent-acp` owns the wire (session lifecycle + pure `session/update` →
//! [`MappedUpdate`] mapping); this module owns the side effects it cannot: it
//! publishes the mapped updates onto the [`EventBus`] (append-then-broadcast, so
//! subscribed clients receive `events.event`) and accumulates the assistant
//! transcript into the append-only `agent_message` log. Exactly one terminal
//! `agent:stream:end` is emitted per turn — `complete` and `error` both map to it
//! (PROTOCOL §7).

use std::path::PathBuf;

use intent_acp::session::{
    self, ContentBlock, InitializeResponse, MappedUpdate, McpServer, StopReason,
};
use intent_acp::{Connection, IncomingNotification};
use intent_core::events::{
    AGENT_FAILED, AGENT_IDLE, AGENT_STREAM_CHUNK, AGENT_STREAM_END, AGENT_TOOL_CALL,
};
use intent_core::{now_iso, ActorType, AgentId, Error, EventActor, Result, WorkspaceId};
use intent_store::NewEvent;
use serde_json::{json, Value};
use tokio::sync::mpsc;

use crate::Services;

#[cfg(test)]
mod tests;

/// Accumulates streamed assistant content into one transcript message per turn,
/// coalescing consecutive text chunks into a single text block.
#[derive(Default)]
struct Transcript {
    blocks: Vec<Value>,
    text: String,
}

impl Transcript {
    fn push_text(&mut self, t: &str) {
        self.text.push_str(t);
    }

    fn push_block(&mut self, block: Value) {
        self.flush_text();
        self.blocks.push(block);
    }

    fn flush_text(&mut self) {
        if !self.text.is_empty() {
            self.blocks
                .push(json!({ "type": "text", "text": std::mem::take(&mut self.text) }));
        }
    }

    fn into_blocks(mut self) -> Vec<Value> {
        self.flush_text();
        self.blocks
    }
}

/// Extract a `lastResponseSummary` for the `agent:idle` payload from the turn's
/// assistant text blocks (mirrors the TS `emitAgentIdleEvent` `finalMessage`
/// summary): join the text blocks and keep the trailing 500 characters — the
/// tail is the meaningful completion, not the "I'll start by…" preamble.
/// `None` when the turn produced no text.
fn last_response_summary(blocks: &[Value]) -> Option<String> {
    let text = blocks
        .iter()
        .filter(|b| b.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|b| b.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join(" ");
    if text.is_empty() {
        return None;
    }
    let chars: Vec<char> = text.chars().collect();
    if chars.len() > 500 {
        let tail: String = chars[chars.len() - 500..].iter().collect();
        Some(format!("...{tail}"))
    } else {
        Some(text)
    }
}

/// The `agent` actor stamped on streaming events (carries the agent id).
fn agent_actor(agent_id: &AgentId) -> EventActor {
    EventActor {
        actor_type: ActorType::Agent,
        id: Some(agent_id.0.clone()),
        ..Default::default()
    }
}

impl Services {
    /// Open a new ACP session and persist its id as `AgentSession.acpSessionId`
    /// (write-once, for later resume) (§6.5).
    pub async fn open_acp_session(
        &self,
        conn: &Connection,
        agent_id: &AgentId,
        cwd: impl Into<PathBuf>,
        mcp_servers: Vec<McpServer>,
    ) -> Result<String> {
        let resp = session::new_session(conn, cwd, mcp_servers)
            .await
            .map_err(|e| Error::Internal(format!("session/new failed: {e}")))?;
        let acp_session_id = resp.session_id.0.to_string();
        self.store
            .set_acp_session_id(agent_id, &acp_session_id)
            .await?;
        Ok(acp_session_id)
    }

    /// Open a FRESH ACP session that REPLACES a lost/unsupported stored id (the
    /// resume-impossible fallback): `session/new` then compare-and-swap the
    /// persisted `acpSessionId` from `expected_old` (the id we just failed to
    /// load) to the fresh one. Unlike [`open_acp_session`] (write-once first-set)
    /// this is used ONLY when resume is impossible — `loadSession` unsupported or
    /// `session/load` failed (§6.5). The CAS keeps the id canonical: if a
    /// concurrent recreate already swapped it, the stored value is returned and
    /// reused instead of being clobbered. Returns the canonical `acpSessionId`.
    pub async fn recreate_acp_session(
        &self,
        conn: &Connection,
        agent_id: &AgentId,
        expected_old: &str,
        cwd: impl Into<PathBuf>,
        mcp_servers: Vec<McpServer>,
    ) -> Result<String> {
        let resp = session::new_session(conn, cwd, mcp_servers)
            .await
            .map_err(|e| Error::Internal(format!("session/new failed: {e}")))?;
        let new_acp_session_id = resp.session_id.0.to_string();
        let canonical = self
            .store
            .replace_acp_session_id(agent_id, expected_old, &new_acp_session_id)
            .await?;
        Ok(canonical)
    }

    /// Resume the agent's persisted `acpSessionId` via `session/load`, but only
    /// when one was stored and the agent advertised the `loadSession` capability.
    /// Returns the resumed id, or `None` when resume is not possible (§6.5).
    pub async fn resume_acp_session(
        &self,
        conn: &Connection,
        init: &InitializeResponse,
        agent_id: &AgentId,
        cwd: impl Into<PathBuf>,
        mcp_servers: Vec<McpServer>,
    ) -> Result<Option<String>> {
        let stored = self.store.get_agent_session(agent_id).await?;
        let Some(acp_session_id) = stored.acp_session_id else {
            return Ok(None);
        };
        if !session::supports_load_session(init) {
            return Ok(None);
        }
        session::load_session(conn, &acp_session_id, cwd, mcp_servers)
            .await
            .map_err(|e| Error::Internal(format!("session/load failed: {e}")))?;
        Ok(Some(acp_session_id))
    }

    /// Discard the `session/update` burst that `session/load` replays after a
    /// successful resume: auggie re-streams the prior conversation as
    /// notifications written to the wire *before* `session/load` returns, so they
    /// buffer in the agent handle's unbounded channel. Left in place they would
    /// leak into the next [`run_prompt_turn`](Self::run_prompt_turn), re-emitting
    /// old messages as live `agent:stream:chunk` events and re-accumulating them
    /// into the transcript. Draining them here mirrors TS's "drop `session/update`
    /// when there is no active streaming handler" gate (acp-provider.ts).
    ///
    /// Bounded so it cannot hang: empty whatever is already buffered with
    /// non-blocking `try_recv`, then wait out a short settle window for stragglers
    /// that may land just after `load_session` resolved (a per-message `recv`
    /// timeout; stop once the channel stays quiet), capping the total wait. The
    /// per-agent single-flight slot serialises this, so a brief block on the
    /// resume path is acceptable.
    pub(crate) async fn drain_replay_notifications(
        notifications: &mut mpsc::UnboundedReceiver<IncomingNotification>,
    ) {
        use tokio::time::{timeout, Duration, Instant};
        const SETTLE: Duration = Duration::from_millis(50);
        const CAP: Duration = Duration::from_millis(500);
        let deadline = Instant::now() + CAP;
        loop {
            while notifications.try_recv().is_ok() {}
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match timeout(SETTLE.min(remaining), notifications.recv()).await {
                Ok(Some(_)) => continue, // a straggler arrived → keep draining
                Ok(None) => break,       // channel closed
                Err(_) => break,         // quiet for the settle window → done
            }
        }
    }

    /// Drive a `session/prompt` turn: stream `session/update`s onto the bus and
    /// accumulate the transcript while the turn runs, then append the assistant
    /// message and emit the single terminal `agent:stream:end`. Returns the
    /// agent's [`StopReason`] (§6.5/§6.6).
    pub async fn run_prompt_turn(
        &self,
        conn: &Connection,
        notifications: &mut mpsc::UnboundedReceiver<IncomingNotification>,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        acp_session_id: &str,
        prompt: Vec<ContentBlock>,
    ) -> Result<StopReason> {
        let mut transcript = Transcript::default();
        let prompt_fut = session::prompt(conn, acp_session_id, prompt);
        tokio::pin!(prompt_fut);
        let mut closed = false;
        let result = loop {
            tokio::select! {
                res = &mut prompt_fut => break res,
                maybe = notifications.recv(), if !closed => match maybe {
                    Some(note) => {
                        self.route_notification(&note, agent_id, workspace_id, &mut transcript)
                            .await;
                    }
                    None => closed = true,
                },
            }
        };
        // Drain updates buffered before the prompt response resolved.
        while let Ok(note) = notifications.try_recv() {
            self.route_notification(&note, agent_id, workspace_id, &mut transcript)
                .await;
        }
        // Accumulate the assistant message (one per turn) into the append-only log.
        let blocks = transcript.into_blocks();
        let last_response_summary = last_response_summary(&blocks);
        if !blocks.is_empty() {
            self.store
                .append_agent_message(agent_id, "assistant", &Value::Array(blocks), &now_iso())
                .await?;
        }
        // Exactly ONE terminal stream:end — complete and error both map here (§7).
        self.publish_agent_event(
            workspace_id,
            agent_id,
            AGENT_STREAM_END,
            json!({ "agentId": agent_id.0 }),
        )
        .await;
        // Session-completion lifecycle signal, emitted AFTER the terminal
        // stream:end (the auto-subscription wake keys off this). A normal
        // turn-end goes idle (`agent:idle`); a turn error maps to `agent:failed`.
        // The interrupt/resume path never reaches here — `interrupt()` aborts
        // this worker before the turn resolves and emits only `stream:end` — so
        // `agent:idle` is suppressed for interrupted agents (mirrors the TS
        // `emitAgentIdleEvent` interrupt suppression).
        match &result {
            Ok(stop_reason) => {
                let mut data = json!({
                    "agentId": agent_id.0,
                    "reason": "stream_complete",
                    "finishReason": stop_reason,
                    "status": "idle",
                });
                if let Some(summary) = last_response_summary {
                    data["lastResponseSummary"] = Value::String(summary);
                }
                self.publish_agent_event(workspace_id, agent_id, AGENT_IDLE, data)
                    .await;
            }
            Err(e) => {
                self.publish_agent_event(
                    workspace_id,
                    agent_id,
                    AGENT_FAILED,
                    json!({ "agentId": agent_id.0, "error": e.to_string() }),
                )
                .await;
            }
        }
        result.map_err(|e| Error::Internal(format!("session/prompt failed: {e}")))
    }

    /// Map one `session/update` notification and publish/accumulate its effects.
    async fn route_notification(
        &self,
        note: &IncomingNotification,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        transcript: &mut Transcript,
    ) {
        let Some(mapped) = session::map_notification(note) else {
            return;
        };
        match mapped {
            MappedUpdate::Chunk { content, text } => {
                match text {
                    Some(t) => transcript.push_text(&t),
                    None => transcript.push_block(content.clone()),
                }
                self.publish_agent_event(
                    workspace_id,
                    agent_id,
                    AGENT_STREAM_CHUNK,
                    json!({ "agentId": agent_id.0, "content": content }),
                )
                .await;
            }
            MappedUpdate::ToolCall(tc) => {
                let mut data = json!({
                    "toolName": tc.tool_name,
                    "toolKind": tc.tool_kind,
                    "input": tc.input,
                    "status": tc.status,
                });
                if let Some(output) = tc.output {
                    data["output"] = output;
                }
                self.publish_agent_event(workspace_id, agent_id, AGENT_TOOL_CALL, data)
                    .await;
            }
        }
    }

    /// Build and publish an agent streaming event onto the bus (§6.6/§10).
    /// `pub(crate)` so the [`AgentManager`] stop path can emit the terminal
    /// `agent:stream:end` when it interrupts a turn (the worker that would
    /// otherwise emit it is aborted).
    pub(crate) async fn publish_agent_event(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        event_type: &str,
        data: Value,
    ) {
        let event = NewEvent {
            workspace_id: workspace_id.clone(),
            timestamp: now_iso(),
            event_type: event_type.to_string(),
            actor: agent_actor(agent_id),
            session_id: Some(agent_id.0.clone()),
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data,
        };
        crate::publish_event(&self.event_bus, event).await;
    }
}
