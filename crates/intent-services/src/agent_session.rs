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

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use intent_acp::session::{
    self, ContentBlock, InitializeResponse, MappedToolCall, MappedUpdate, McpServer,
    SessionModeState, StopReason,
};
use intent_acp::{Connection, IncomingNotification};
use intent_core::events::{
    AGENT_FAILED, AGENT_IDLE, AGENT_STREAM_CHUNK, AGENT_STREAM_END, AGENT_STREAM_STATUS,
    AGENT_TOOL_CALL,
};
use intent_core::{
    now_epoch_ms, now_iso, ActorType, AgentId, Error, EventActor, Result, WorkspaceId,
};
use intent_store::NewEvent;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::Services;

#[cfg(test)]
mod tests;

/// Result of opening or resuming an ACP session: the canonical `acpSessionId`
/// (persisted on `AgentSession`) plus the modes the provider advertised in the
/// same response, so the caller can pick a permissive `session/set_mode` target
/// only from `availableModes` rather than blindly asking for a mode the agent
/// never offered.
#[derive(Debug, Clone)]
pub struct AcpSessionOpened {
    /// The canonical `acpSessionId` to drive future turns.
    pub session_id: String,
    /// The modes the provider advertised in `session/new` / `session/load`, if
    /// any. `None` when the provider omitted the field or when a concurrent
    /// recreate won the CAS (the modes we captured belong to the wrong
    /// session).
    pub modes: Option<SessionModeState>,
}

/// Accumulates streamed assistant content into one transcript message per turn,
/// coalescing consecutive text chunks into a single text block and pushing
/// `tool_use`/`tool_result` blocks for tool calls (CS-0 D6). Every block is
/// stamped with a stable id `{messageId}:{blockIndex}` (CS-0 D1), where
/// `messageId` is the assistant `AgentMessage` id minted at turn start; blocks
/// are append-only so a block's index (and thus id) is fixed once assigned.
struct Transcript {
    /// Assistant `AgentMessage` id minted at turn start (the block-id prefix).
    message_id: String,
    blocks: Vec<Value>,
    text: String,
    /// `toolCallId` → index of its `tool_use` block (for status patching).
    tool_use_index: HashMap<String, usize>,
    /// `toolCallId` → index of its `tool_result` block (append-once, then patch).
    tool_result_index: HashMap<String, usize>,
}

impl Transcript {
    fn new(message_id: String) -> Self {
        Self {
            message_id,
            blocks: Vec::new(),
            text: String::new(),
            tool_use_index: HashMap::new(),
            tool_result_index: HashMap::new(),
        }
    }

    /// The stable block id for a 0-based block index (`{messageId}:{index}`).
    fn block_id(&self, index: usize) -> String {
        format!("{}:{index}", self.message_id)
    }

    /// Index the currently pending (or next) coalesced text block will occupy
    /// once flushed — the same value for every consecutive text chunk, so they
    /// share one block id.
    fn current_text_index(&self) -> usize {
        self.blocks.len()
    }

    fn push_text(&mut self, t: &str) {
        self.text.push_str(t);
    }

    /// Push a non-text passthrough content block, stamping its id; returns its
    /// index.
    fn push_block(&mut self, mut block: Value) -> usize {
        self.flush_text();
        let index = self.blocks.len();
        if let Some(obj) = block.as_object_mut() {
            obj.insert("id".to_string(), Value::String(self.block_id(index)));
        }
        self.blocks.push(block);
        index
    }

    fn flush_text(&mut self) {
        if !self.text.is_empty() {
            let index = self.blocks.len();
            let id = self.block_id(index);
            self.blocks
                .push(json!({ "type": "text", "id": id, "text": std::mem::take(&mut self.text) }));
        }
    }

    /// Record a tool call into the transcript (CS-0 D6). On first sight of a
    /// `toolCallId`, flush any open text and push a `tool_use` block; on repeats,
    /// patch its `metadata.status`. When the tool reaches `completed`/`error`
    /// WITH output, append (then patch) a matching `tool_result` block. Returns
    /// the index of the `tool_use` block (the block the `agent:tool:call` event
    /// is enriched against).
    fn record_tool(&mut self, tc: &MappedToolCall) -> usize {
        let use_index = match self.tool_use_index.get(&tc.tool_call_id) {
            Some(&i) => {
                if let Some(meta) = self.blocks[i]
                    .get_mut("metadata")
                    .and_then(Value::as_object_mut)
                {
                    meta.insert("status".to_string(), Value::String(tc.status.to_string()));
                }
                i
            }
            None => {
                self.flush_text();
                let index = self.blocks.len();
                let id = self.block_id(index);
                // Shared factory (§7.1): keeps the persisted block
                // byte-identical to the live `chat.subscribe` delta.
                self.blocks.push(crate::tool_block::build_tool_use_block(
                    &id,
                    &tc.tool_name,
                    &tc.title,
                    tc.input.clone(),
                    &tc.tool_call_id,
                    tc.tool_kind,
                    tc.status,
                ));
                self.tool_use_index.insert(tc.tool_call_id.clone(), index);
                index
            }
        };
        let completed = tc.status == "completed" || tc.status == "error";
        if completed {
            if let Some(output) = &tc.output {
                let is_error = tc.status == "error";
                match self.tool_result_index.get(&tc.tool_call_id) {
                    Some(&ri) => {
                        if let Some(obj) = self.blocks[ri].as_object_mut() {
                            obj.insert("output".to_string(), output.clone());
                            obj.insert("is_error".to_string(), Value::Bool(is_error));
                        }
                    }
                    None => {
                        self.flush_text();
                        let rindex = self.blocks.len();
                        let rid = self.block_id(rindex);
                        self.blocks.push(json!({
                            "type": "tool_result",
                            "id": rid,
                            "tool_use_id": tc.tool_call_id,
                            "output": output,
                            "is_error": is_error,
                        }));
                        self.tool_result_index
                            .insert(tc.tool_call_id.clone(), rindex);
                    }
                }
            }
        }
        use_index
    }

    fn into_blocks(mut self) -> Vec<Value> {
        self.flush_text();
        self.blocks
    }

    /// A non-consuming snapshot of the coalesced blocks AS THEY STAND mid-turn
    /// (CS-0 D5): the pushed blocks plus, when text is pending, the synthetic
    /// `text` block it will flush into (same index/id it would ultimately take).
    /// Used to publish the in-flight partial into the per-agent live-turn slot so
    /// a `chat.subscribe` arriving mid-turn can reconstruct it.
    fn snapshot_blocks(&self) -> Vec<Value> {
        let mut blocks = self.blocks.clone();
        if !self.text.is_empty() {
            let index = blocks.len();
            blocks.push(json!({
                "type": "text",
                "id": self.block_id(index),
                "text": self.text.clone(),
            }));
        }
        blocks
    }
}

/// The per-agent in-flight ("live") turn slot (CS-0 D5): the assistant message
/// id minted at turn start plus a non-consuming [`Transcript::snapshot_blocks`]
/// of the partial assistant content AS IT STANDS. Published while a
/// `session/prompt` turn streams so a `chat.subscribe` arriving mid-turn can
/// reconstruct the in-flight message; cleared (by [`LiveTurnGuard`] and on the
/// happy path before `stream:end`) once the turn's message is persisted.
#[derive(Clone)]
pub(crate) struct LiveTurn {
    pub(crate) message_id: String,
    pub(crate) blocks: Vec<Value>,
}

/// Per-agent map of live turn slots, shared across [`Services`] clones so the
/// `chat.subscribe` read door and the [`run_prompt_turn`](Services::run_prompt_turn)
/// writer observe the same state.
pub(crate) type LiveTurns = Arc<Mutex<HashMap<AgentId, LiveTurn>>>;

/// RAII guard that clears an agent's live-turn slot when a turn ends — including
/// the interrupt/abort path, where the worker future is dropped before
/// `stream:end` is reached. Without it an aborted turn would leave a stale
/// in-flight message in the snapshot forever.
pub(crate) struct LiveTurnGuard<'a> {
    live_turns: &'a LiveTurns,
    agent_id: AgentId,
}

impl Drop for LiveTurnGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut slots) = self.live_turns.lock() {
            slots.remove(&self.agent_id);
        }
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
pub(crate) fn agent_actor(agent_id: &AgentId) -> EventActor {
    EventActor {
        actor_type: ActorType::Agent,
        id: Some(agent_id.0.clone()),
        ..Default::default()
    }
}

impl Services {
    /// Begin a live-turn slot for `agent_id` (CS-0 D5): seed it with the freshly
    /// minted assistant `message_id` and no blocks yet, returning a
    /// [`LiveTurnGuard`] that clears the slot on drop (abort-safe). The slot is
    /// refreshed by [`update_live_turn`](Self::update_live_turn) as content streams.
    pub(crate) fn begin_live_turn(
        &self,
        agent_id: &AgentId,
        message_id: &str,
    ) -> LiveTurnGuard<'_> {
        self.set_live_turn(agent_id, message_id, Vec::new());
        LiveTurnGuard {
            live_turns: &self.live_turns,
            agent_id: agent_id.clone(),
        }
    }

    /// Set/replace an agent's live-turn slot. The streaming path drives this via
    /// [`update_live_turn`](Self::update_live_turn); it is also a test seam for
    /// simulating a mid-turn snapshot without spinning up a real ACP turn.
    pub fn set_live_turn(&self, agent_id: &AgentId, message_id: &str, blocks: Vec<Value>) {
        if let Ok(mut slots) = self.live_turns.lock() {
            slots.insert(
                agent_id.clone(),
                LiveTurn {
                    message_id: message_id.to_string(),
                    blocks,
                },
            );
        }
    }

    /// Refresh the agent's live-turn blocks from the current [`Transcript`] (a
    /// non-consuming [`Transcript::snapshot_blocks`]). No-op if no slot is open.
    fn update_live_turn(&self, agent_id: &AgentId, transcript: &Transcript) {
        if let Ok(mut slots) = self.live_turns.lock() {
            if let Some(slot) = slots.get_mut(agent_id) {
                slot.blocks = transcript.snapshot_blocks();
            }
        }
    }

    /// Clear an agent's live-turn slot (happy-path turn end + test seam). The
    /// [`LiveTurnGuard`] also clears on drop for the interrupt/abort path.
    pub fn clear_live_turn(&self, agent_id: &AgentId) {
        if let Ok(mut slots) = self.live_turns.lock() {
            slots.remove(agent_id);
        }
    }

    /// Read an agent's in-flight turn slot, if a turn is currently streaming.
    pub(crate) fn live_turn(&self, agent_id: &AgentId) -> Option<LiveTurn> {
        self.live_turns.lock().ok()?.get(agent_id).cloned()
    }

    /// Open a new ACP session and persist its id as `AgentSession.acpSessionId`
    /// (write-once, for later resume) (§6.5). Returns the fresh id plus the
    /// modes the provider advertised in `session/new` (used by the caller to
    /// pick a permissive `session/set_mode` target from `availableModes`).
    pub async fn open_acp_session(
        &self,
        conn: &Connection,
        provider_id: &str,
        agent_id: &AgentId,
        cwd: impl Into<PathBuf>,
        mcp_servers: Vec<McpServer>,
    ) -> Result<AcpSessionOpened> {
        // Load the session up front so the store write is scoped to the owning
        // workspace (the store's `set_acp_session_id` now requires it as a
        // defense-in-depth guard). This call is only reached after the caller
        // resolved this agent id inside a workspace-scoped path.
        let workspace_id = self.store.get_agent_session(agent_id).await?.workspace_id;
        self.publish_status_event(
            &workspace_id,
            agent_id,
            "session-create",
            "Creating session\u{2026}",
            "info",
        )
        .await;
        let resp = session::new_session(conn, provider_id, cwd, mcp_servers)
            .await
            .map_err(|e| Error::Internal(format!("session/new failed: {e}")))?;
        let acp_session_id = resp.session_id.0.to_string();
        self.store
            .set_acp_session_id(&workspace_id, agent_id, &acp_session_id)
            .await?;
        Ok(AcpSessionOpened {
            session_id: acp_session_id,
            modes: resp.modes,
        })
    }

    /// Open a FRESH ACP session that REPLACES a lost/unsupported stored id (the
    /// resume-impossible fallback): `session/new` then compare-and-swap the
    /// persisted `acpSessionId` from `expected_old` (the id we just failed to
    /// load) to the fresh one. Unlike [`open_acp_session`] (write-once first-set)
    /// this is used ONLY when resume is impossible — `loadSession` unsupported or
    /// `session/load` failed (§6.5). The CAS keeps the id canonical: if a
    /// concurrent recreate already swapped it, the stored value is returned and
    /// reused instead of being clobbered. Returns the canonical `acpSessionId`
    /// with modes only when the freshly-opened session won the CAS — otherwise
    /// the modes belong to some other session and callers must not act on them.
    pub async fn recreate_acp_session(
        &self,
        conn: &Connection,
        provider_id: &str,
        agent_id: &AgentId,
        expected_old: &str,
        cwd: impl Into<PathBuf>,
        mcp_servers: Vec<McpServer>,
    ) -> Result<AcpSessionOpened> {
        // Load the session up front so the CAS replace is scoped to the owning
        // workspace (see [`open_acp_session`]).
        let workspace_id = self.store.get_agent_session(agent_id).await?.workspace_id;
        self.publish_status_event(
            &workspace_id,
            agent_id,
            "session-create",
            "Creating session\u{2026}",
            "info",
        )
        .await;
        let resp = session::new_session(conn, provider_id, cwd, mcp_servers)
            .await
            .map_err(|e| Error::Internal(format!("session/new failed: {e}")))?;
        let new_acp_session_id = resp.session_id.0.to_string();
        let canonical = self
            .store
            .replace_acp_session_id(&workspace_id, agent_id, expected_old, &new_acp_session_id)
            .await?;
        // On CAS loss the canonical id belongs to a session we did not open;
        // our modes are meaningless for it and would target the wrong sid.
        let modes = if canonical == new_acp_session_id {
            resp.modes
        } else {
            None
        };
        Ok(AcpSessionOpened {
            session_id: canonical,
            modes,
        })
    }

    /// Resume the agent's persisted `acpSessionId` via `session/load`, but only
    /// when one was stored and the agent advertised the `loadSession` capability.
    /// Returns the resumed id plus the modes the provider advertised in
    /// `session/load`, or `None` when resume is not possible (§6.5).
    pub async fn resume_acp_session(
        &self,
        conn: &Connection,
        provider_id: &str,
        init: &InitializeResponse,
        agent_id: &AgentId,
        cwd: impl Into<PathBuf>,
        mcp_servers: Vec<McpServer>,
    ) -> Result<Option<AcpSessionOpened>> {
        let stored = self.store.get_agent_session(agent_id).await?;
        let workspace_id = stored.workspace_id.clone();
        let Some(acp_session_id) = stored.acp_session_id else {
            return Ok(None);
        };
        if !session::supports_load_session(init) {
            return Ok(None);
        }
        self.publish_status_event(
            &workspace_id,
            agent_id,
            "session-load",
            "Resuming session\u{2026}",
            "info",
        )
        .await;
        let resp = session::load_session(conn, provider_id, &acp_session_id, cwd, mcp_servers)
            .await
            .map_err(|e| Error::Internal(format!("session/load failed: {e}")))?;
        Ok(Some(AcpSessionOpened {
            session_id: acp_session_id,
            modes: resp.modes,
        }))
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
        // Mint the assistant message id at turn START (CS-0 D1) so streaming
        // block ids `{messageId}:{index}` match the blocks ultimately persisted.
        let message_id = Uuid::now_v7().to_string();
        let mut transcript = Transcript::new(message_id.clone());
        // Publish the in-flight turn so a `chat.subscribe` arriving mid-turn can
        // reconstruct the partial message (CS-0 D5). The guard clears the slot on
        // ANY exit — including the interrupt/abort path that drops this worker
        // before `stream:end` — so subscribers never see a stale in-flight turn.
        let _live_guard = self.begin_live_turn(agent_id, &message_id);
        // Pre-first-token turn-startup hint: the FE renders "Sent prompt…" next
        // to the spinner until the first `agent:stream:chunk` clears it. Emitted
        // exactly once per turn immediately before dispatching `session/prompt`.
        self.publish_status_event(
            workspace_id,
            agent_id,
            "prompt",
            "Sent prompt\u{2026}",
            "info",
        )
        .await;
        // Activity tracker for idle-based timeout: reset on every notification.
        let activity = session::ActivityTracker::new();
        let prompt_fut = session::prompt(conn, acp_session_id, prompt, &activity);
        tokio::pin!(prompt_fut);
        let mut closed = false;
        let result = loop {
            tokio::select! {
                res = &mut prompt_fut => break res,
                maybe = notifications.recv(), if !closed => match maybe {
                    Some(note) => {
                        activity.touch();
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
                .append_agent_message_with_id(
                    agent_id,
                    &message_id,
                    "assistant",
                    &Value::Array(blocks),
                    None,
                    &now_iso(),
                )
                .await?;
        }
        // The turn's message is now durable: clear the live-turn slot so the next
        // `chat.subscribe` snapshot reflects the persisted message (not a stale
        // in-flight copy) BEFORE the terminal `stream:end` is observed. The guard
        // remains as the abort-path fallback.
        self.clear_live_turn(agent_id);
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
        //
        // PROTOCOL §5.5/§6.5 invariant: `agent:idle` is **also** suppressed
        // while the agent has at least one ready-to-send queued message — the
        // drain loop is about to flip the next message to in-flight, so the
        // agent is not actually idle. A queue containing only under-edit
        // messages (`editing = true`) is treated as empty for this check.
        match &result {
            Ok(stop_reason) if !self.has_ready_to_send(agent_id) => {
                let mut data = json!({
                    "agentId": agent_id.0,
                    "reason": "stream_complete",
                    "finishReason": stop_reason,
                    "status": "idle",
                });
                if let Some(summary) = last_response_summary {
                    data["lastResponseSummary"] = Value::String(summary);
                }
                // DELIV-1: enrich the idle payload with `agentName` (so
                // subscribers don't fall back to a generic "Agent" label)
                // and — when the child persisted one via `agent.reportToParent`
                // — the completion `report`. The lookup is a single indexed
                // row read per idle event; a store error is swallowed and the
                // event still fires with the base payload.
                if let Ok(session) = self.store.get_agent_session(agent_id).await {
                    data["agentName"] = Value::String(session.name);
                    if let Some(report) = session.completion_report {
                        data["report"] = Value::String(report);
                    }
                }
                // DURABLE-BEFORE-OBSERVABLE: record delegation-group completion
                // BEFORE publishing the idle event so the persisted state is
                // correct if the daemon is killed immediately after the event.
                self.record_group_completion_pre_publish(workspace_id, agent_id, &data)
                    .await;
                self.publish_agent_event(workspace_id, agent_id, AGENT_IDLE, data)
                    .await;
            }
            Ok(_) => {
                // Ready-to-send messages remain — stay busy and skip the idle
                // signal so the FE/auto-commit do not key off a transient
                // mid-drain "idle" snapshot. The terminal `agent:idle` will
                // fire when the queue is truly drained.
                tracing::debug!(
                    agent = %agent_id,
                    "agent:idle suppressed — ready-to-send queue non-empty",
                );
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
        let message_id = transcript.message_id.clone();
        match mapped {
            MappedUpdate::Chunk { content, text } => {
                // Accumulate into the transcript and compute the block index this
                // chunk lands at; consecutive text chunks coalesce onto one index
                // (and thus one stable block id), a non-text block starts a new one.
                let (block_index, block_type) = match &text {
                    Some(t) => {
                        let index = transcript.current_text_index();
                        transcript.push_text(t);
                        (index, "text".to_string())
                    }
                    None => {
                        let block_type = content
                            .get("type")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown")
                            .to_string();
                        (transcript.push_block(content.clone()), block_type)
                    }
                };
                // D4: enrich additively — keep `content`, add block identity.
                self.publish_agent_event(
                    workspace_id,
                    agent_id,
                    AGENT_STREAM_CHUNK,
                    json!({
                        "agentId": agent_id.0,
                        "content": content,
                        "messageId": message_id,
                        "blockIndex": block_index,
                        "blockId": transcript.block_id(block_index),
                        "blockType": block_type,
                    }),
                )
                .await;
            }
            MappedUpdate::ToolCall(tc) => {
                // D6: accumulate tool_use/tool_result blocks into the transcript
                // so they persist (and reach `agent.getConversation`).
                let block_index = transcript.record_tool(&tc);
                // D4: enrich additively — keep the existing fields, add agentId,
                // the (previously dropped) toolCallId, and the block identity.
                let mut data = json!({
                    "agentId": agent_id.0,
                    "toolName": tc.tool_name,
                    "title": tc.title,
                    "toolKind": tc.tool_kind,
                    "toolCallId": tc.tool_call_id,
                    "input": tc.input,
                    "status": tc.status,
                    "messageId": message_id,
                    "blockIndex": block_index,
                    "blockId": transcript.block_id(block_index),
                });
                if let Some(output) = tc.output {
                    data["output"] = output;
                }
                self.publish_agent_event(workspace_id, agent_id, AGENT_TOOL_CALL, data)
                    .await;
            }
        }
        // Refresh the live-turn slot with the partial transcript so a mid-turn
        // `chat.subscribe` snapshot reflects content streamed so far (CS-0 D5).
        self.update_live_turn(agent_id, transcript);
    }

    /// Publish an `agent:stream:status` turn-startup hint on the bus (PROTOCOL
    /// §6.5 / §7). Mirrors the TS reference `acp-provider.ts` `emitStatus()`
    /// call sites: the FE renders the phase message next to the pre-first-token
    /// spinner and clears it on the first chunk / `agent:stream:end` /
    /// `agent:failed`. Self-sufficient payload — no follow-up fetch required.
    /// `pub(crate)` so the [`AgentManager`] `launch` / `init` emit sites can
    /// hit the same helper.
    pub(crate) async fn publish_status_event(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        phase: &str,
        message: &str,
        level: &str,
    ) {
        self.publish_agent_event(
            workspace_id,
            agent_id,
            AGENT_STREAM_STATUS,
            json!({
                "agentId": agent_id.0,
                "workspaceId": workspace_id,
                "phase": phase,
                "message": message,
                "level": level,
                "timestamp": now_epoch_ms(),
            }),
        )
        .await;
    }

    /// Build and publish an agent streaming event onto the bus (§6.6/§10).
    /// `pub(crate)` so the [`AgentManager`] stop path can emit the terminal
    /// `agent:stream:end` when it interrupts a turn (the worker that would
    /// otherwise emit it is aborted). Routes `agent:stream:chunk` through the
    /// transient (broadcast-only, never persisted) path; all other event types
    /// persist durably.
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
        // Route stream chunks through the transient path (broadcast-only);
        // persist all other agent events (stream:status, stream:end, tool:call,
        // lifecycle, etc.) for durable audit trail.
        if event_type == AGENT_STREAM_CHUNK {
            crate::publish_event_transient(&self.event_bus, event);
        } else {
            crate::publish_event(&self.event_bus, event).await;
        }
    }
}
