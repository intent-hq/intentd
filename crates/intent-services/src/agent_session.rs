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
use intent_core::events::{AGENT_STREAM_CHUNK, AGENT_STREAM_END, AGENT_TOOL_CALL};
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
